#!/usr/bin/env bash
# =============================================================================
# slackslaw.sh — slackslaw, the CLI for Slack (wraps slackdump)
#
# Mirrors your Slack workspaces into local SQLite for offline search,
# agent-friendly querying, and incremental sync.
#
# Usage:
#   slackslaw auth                          # add / re-auth a workspace
#   slackslaw workspaces                    # list configured workspaces
#   slackslaw workspaces remove <name>      # remove a workspace
#   slackslaw sync --full                   # full archive of all workspaces
#   slackslaw sync                          # incremental sync (new msgs only)
#   slackslaw sync --channel "#ops"         # sync a single channel
#   slackslaw sync --since 30d              # time-bounded sync
#   slackslaw sync --quiet                  # cron-friendly (silent on success)
#   slackslaw whatsnew                      # print msgs since last sync
#   slackslaw whatsnew --json               # same, JSON output (one obj/line)
#   slackslaw search "expr"                 # FTS search across all workspaces
#   slackslaw search "expr" --after 7d      # only recent messages
#   slackslaw search "expr" --before DATE   # only messages before date
#   slackslaw search "expr" --format slack  # raw Slack mrkdwn output
#   slackslaw threads <url>                 # dump a thread by Slack URL
#   slackslaw stats                         # detailed archive statistics
#   slackslaw export --channel "#general"   # export channel to markdown
#   slackslaw tail -f                       # live-watch for new messages
#   slackslaw context --since 7d            # LLM-ready JSON context
#   slackslaw ask "question"                # RAG over the archive
#   slackslaw mcp                           # start MCP server
#   slackslaw gc                            # vacuum databases
#   slackslaw repair                        # check DB integrity
#   slackslaw sql 'SELECT ...'              # raw SQL against local DB(s)
#   slackslaw status                        # archive stats per workspace
#   slackslaw doctor                        # check dependencies & config
#
# Data lives in ~/.slackslaw/
#   workspaces/<n>/          — slackdump archive dir per workspace
#   workspaces/<n>/*.db      — SQLite database (auto-discovered)
#   state.json                  — last-sync timestamps
#   config.json                 — workspace list
#
# Key slackdump facts (v4):
#   - -workspace and -y are per-subcommand flags in v4 — they go AFTER
#     the subcommand name, not before:
#       slackdump archive -workspace <ws> -y -o <dir>   ← correct
#       slackdump -workspace <ws> archive ...            ← WRONG (v4)
#   - `archive -workspace <ws> -y -o <dir>` — full sync into <dir>
#   - `resume -workspace <ws> <dir>`         — incremental from checkpoint
#   - The .db file lives inside the output dir, discovered at runtime
#
# =============================================================================

set -euo pipefail

# ── Paths ─────────────────────────────────────────────────────────────────────
SLACKSLAW_DIR="${SLACKSLAW_DIR:-$HOME/.slackslaw}"
WORKSPACES_DIR="$SLACKSLAW_DIR/workspaces"
STATE_FILE="$SLACKSLAW_DIR/state.json"
CONFIG_FILE="$SLACKSLAW_DIR/config.json"
LOG_FILE="$SLACKSLAW_DIR/slackslaw.log"

# ── Colors (disabled when not a tty or NO_COLOR is set) ──────────────────────
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[0;32m'
  YELLOW=$'\033[0;33m'; CYAN=$'\033[0;36m'; RED=$'\033[0;31m'; RESET=$'\033[0m'
else
  BOLD=''; DIM=''; GREEN=''; YELLOW=''; CYAN=''; RED=''; RESET=''
fi

# ── Helpers ───────────────────────────────────────────────────────────────────
log()   { echo -e "${DIM}[$(date '+%H:%M:%S')]${RESET} $*" | tee -a "$LOG_FILE"; }
info()  { echo -e "${GREEN}→${RESET} $*"; }
warn()  { echo -e "${YELLOW}⚠${RESET}  $*" >&2; }
error() { echo -e "${RED}✗${RESET}  $*" >&2; }
die()   { error "$*"; exit 1; }
header(){ echo -e "\n${BOLD}${CYAN}$*${RESET}"; }

# JSON helpers (pure python3, no jq required)
json_get() {
  python3 - "$1" "$2" <<'PYEOF' 2>/dev/null
import json,sys
path,key=sys.argv[1],sys.argv[2]
try:
    d=json.load(open(path))
    v=d.get(key,'')
    print(v if v is not None else '')
except:
    pass
PYEOF
}

json_set() {
  python3 - "$1" "$2" "$3" <<'PYEOF' 2>/dev/null
import json,sys
path,key,val=sys.argv[1],sys.argv[2],sys.argv[3]
try:
    d=json.load(open(path))
except:
    d={}
d[key]=val
json.dump(d,open(path,'w'),indent=2)
PYEOF
}

json_array_get() {
  python3 - "$1" "$2" <<'PYEOF' 2>/dev/null
import json,sys
path,key=sys.argv[1],sys.argv[2]
try:
    d=json.load(open(path))
    for v in d.get(key,[]):
        print(v)
except:
    pass
PYEOF
}

json_array_append() {
  python3 - "$1" "$2" "$3" <<'PYEOF' 2>/dev/null
import json,sys
path,key,val=sys.argv[1],sys.argv[2],sys.argv[3]
try:
    d=json.load(open(path))
except:
    d={}
arr=d.get(key,[])
if val not in arr:
    arr.append(val)
d[key]=arr
json.dump(d,open(path,'w'),indent=2)
PYEOF
}

# Parse relative duration (e.g. "7d", "24h", "30m") to unix timestamp (now - duration)
parse_duration_to_ts() {
  local input="$1"
  local num="${input%[dhm]}" unit="${input: -1}"
  local secs
  case "$unit" in
    d) secs=$((num * 86400)) ;;
    h) secs=$((num * 3600)) ;;
    m) secs=$((num * 60)) ;;
    *) die "Invalid duration: $input (use e.g. 7d, 24h, 30m)" ;;
  esac
  python3 -c "import time; print(int(time.time()) - $secs)"
}

# Parse a date or duration string to a unix timestamp
# Accepts: "7d", "24h", "30m" (relative) or "2024-01-15" (absolute date)
parse_date_to_ts() {
  local input="$1"
  if [[ "$input" =~ ^[0-9]+[dhm]$ ]]; then
    parse_duration_to_ts "$input"
  elif [[ "$input" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    python3 -c "
import datetime, sys
dt = datetime.datetime.strptime('$input', '%Y-%m-%d')
print(int(dt.replace(tzinfo=datetime.timezone.utc).timestamp()))
"
  else
    die "Invalid date/duration: $input (use e.g. 7d, 2024-01-15)"
  fi
}

json_remove_from_array() {
  python3 - "$1" "$2" "$3" <<'PYEOF' 2>/dev/null
import json, sys
path, key, val = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    d = json.load(open(path))
except:
    d = {}
arr = d.get(key, [])
arr = [v for v in arr if v != val]
d[key] = arr
json.dump(d, open(path, "w"), indent=2)
PYEOF
}

# ── "Mine" filter builder ─────────────────────────────────────────────────────

# Build SQL clause for --mine filtering.
# Matches messages where the authenticated user participated, is mentioned,
# is subscribed, starred, or where a configured keyword appears.
# Args: $1=db $2=msg_table $3=text_col $4=user_expr
# Outputs the SQL clause (without leading AND) to stdout.
build_mine_clause() {
  local db="$1" msg_table="$2" text_col="$3" user_expr="$4"

  local my_uid
  my_uid=$(sqlite3 "$db" "SELECT USER_ID FROM WORKSPACE LIMIT 1;" 2>/dev/null || true)
  [[ -z "$my_uid" ]] && return

  # Collect configured keywords
  local keywords_sql=""
  local kw
  while IFS= read -r kw; do
    [[ -z "$kw" ]] && continue
    local kw_escaped
    kw_escaped=$(echo "$kw" | sed "s/'/''/g")
    keywords_sql+="OR LOWER(m.${text_col}) LIKE LOWER('%${kw_escaped}%')
          "
  done < <(json_array_get "$CONFIG_FILE" "keywords")

  cat <<SQLEOF
(
          -- I'm the author of this message
          ${user_expr} = '${my_uid}'
          -- or I'm mentioned in this message
          OR m.${text_col} LIKE '%<@${my_uid}>%'
          -- or I'm subscribed/following this thread
          OR json_extract(m.DATA, '\$.subscribed') = true
          -- or this message is starred/bookmarked
          OR json_extract(m.DATA, '\$.is_starred') = true
          -- or a configured keyword matches
          ${keywords_sql}-- or the message belongs to a thread where I participated or am mentioned
          OR EXISTS (
            SELECT 1 FROM ${msg_table} t
            WHERE t.CHANNEL_ID = m.CHANNEL_ID
              AND (
                -- same thread (match via THREAD_TS)
                (m.THREAD_TS IS NOT NULL AND t.THREAD_TS = m.THREAD_TS)
                -- or message is a thread parent and t is a reply
                OR (m.THREAD_TS IS NULL AND t.THREAD_TS = m.TS)
              )
              AND (
                json_extract(t.DATA, '\$.user') = '${my_uid}'
                OR t.${text_col} LIKE '%<@${my_uid}>%'
              )
          )
        )
SQLEOF
}

# ── Pretty-printing (non-JSON output) ────────────────────────────────────────

# Format rows for human-readable output.
# Handles multiline message text, prettifies @mentions, channel names, URLs.
# Input: sqlite3 -json output on stdin
# Args: $1=db_path $2=user_table $3=format ("list" for whatsnew, "detail" for search/unread)
#        $4=BOLD $5=DIM $6=GREEN $7=RESET $8=CYAN
format_rows() {
  local db="$1" user_table="$2" format="$3" json_data="$4"
  python3 - "$db" "$user_table" "$format" "$BOLD" "$DIM" "$GREEN" "$RESET" "$CYAN" "$json_data" <<'PYEOF'
import json, re, sqlite3, sys
from datetime import datetime, timezone

db_path, user_table, fmt = sys.argv[1], sys.argv[2], sys.argv[3]
BOLD, DIM, GREEN, RESET, CYAN = sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8]

rows = json.loads(sys.argv[9])
if not rows:
    sys.exit(0)

conn = sqlite3.connect(db_path)

# Build user map
user_map = {}
try:
    for r in conn.execute(f"SELECT ID, USERNAME FROM {user_table} GROUP BY ID"):
        user_map[r[0]] = r[1]
except:
    pass

# Build channel map: ID → (name, is_im, dm_user_id)
ch_map = {}
try:
    for r in conn.execute(
        "SELECT ID, NAME, json_extract(DATA, '$.is_im'), json_extract(DATA, '$.user') "
        "FROM CHANNEL GROUP BY ID"
    ):
        ch_map[r[0]] = (r[1] or "", r[2], r[3] or "")
except:
    pass

conn.close()

def prettify_text(text):
    if not text:
        return ""
    # @mentions
    def repl_mention(m):
        return "@" + user_map.get(m.group(1), m.group(1))
    text = re.sub(r'<@([A-Z0-9]+)(?:\|[^>]*)?>', repl_mention, text)
    # mailto
    text = re.sub(r'<mailto:([^|>]+)\|([^>]+)>', r'\2', text)
    # URLs with label
    def repl_url(m):
        url = re.sub(r'^https?://', '', m.group(1))
        return m.group(2) + " (" + url + ")"
    text = re.sub(r'<(https?://[^|>]+)\|([^>]+)>', repl_url, text)
    # Bare URLs
    text = re.sub(r'<(https?://[^>]+)>', r'\1', text)
    # Channel refs
    text = re.sub(r'<#[A-Z0-9]+\|([^>]+)>', r'#\1', text)
    # Special mentions
    text = re.sub(r'<!([a-z]+)(?:\|[^>]*)?>', r'@\1', text)
    return text

def pretty_channel(name, ch_id):
    # Multi-party DM
    if name.startswith("mpdm-"):
        parts = re.sub(r'^mpdm-', '', name)
        parts = re.sub(r'-\d+$', '', parts)
        return ", ".join(p for p in parts.split("--") if p)
    # Direct message
    if ch_id.startswith("D"):
        info = ch_map.get(ch_id)
        if info and info[2]:
            return "DM: " + user_map.get(info[2], info[2])
        return "DM: " + ch_id
    return "#" + (name or ch_id)

# Pre-process all rows to compute column widths
parsed = []
for r in rows:
    ts = r.get("ts", "") or r.get("TS", "")
    author = r.get("author", "unknown")
    channel = r.get("channel", "")
    ch_id = r.get("channel_id", "") or r.get("CHANNEL_ID", "")
    text = r.get("text", "") or r.get("text_snippet", "") or r.get("TXT", "") or ""
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except:
        dt = ts
    pch = pretty_channel(channel, ch_id)
    ptxt = prettify_text(text)
    parsed.append((dt, pch, author, ptxt))

# Compute fixed column widths (capped to stay reasonable)
W_DATE = 16  # "YYYY-MM-DD HH:MM" is always 16
W_CH = max((len(p[1]) for p in parsed), default=7)
W_CH = max(W_CH, 7)   # min width = len("Channel")
W_CH = min(W_CH, 30)  # cap
W_AU = max((len(p[2]) for p in parsed), default=6)
W_AU = max(W_AU, 6)   # min width = len("Author")
W_AU = min(W_AU, 20)  # cap

# Print header
hdr_date = "Date".ljust(W_DATE)
hdr_ch   = "Channel".ljust(W_CH)
hdr_au   = "Author".ljust(W_AU)
hdr_msg  = "Message"
print(f"{BOLD}{hdr_date}  {hdr_ch}  {hdr_au}  {hdr_msg}{RESET}")
print(f"{DIM}{'─' * W_DATE}  {'─' * W_CH}  {'─' * W_AU}  {'─' * 30}{RESET}")

for dt, pch, author, ptxt in parsed:
    col_date = f"{DIM}{dt.ljust(W_DATE)}{RESET}"
    col_ch   = f"{BOLD}{pch.ljust(W_CH)[:W_CH]}{RESET}"
    col_au   = f"{GREEN}{author.ljust(W_AU)[:W_AU]}{RESET}"
    # First line of message on the same row; continuation lines indented
    lines = ptxt.split("\n")
    indent = W_DATE + W_CH + W_AU + 6  # 6 = three 2-char gaps
    first_line = lines[0] if lines else ""
    print(f"{col_date}  {col_ch}  {col_au}  {first_line}")
    for extra in lines[1:]:
        if extra.strip():
            print(f"{' ' * indent}  {extra}")
PYEOF
}

# Print rows in raw Slack mrkdwn format (no prettify).
# Input: JSON array string as $1
format_rows_slackmd() {
  local json_data="$1"
  python3 -c "
import json, sys
from datetime import datetime, timezone
rows = json.loads(sys.argv[1])
for r in rows:
    ts = r.get('ts', '') or r.get('TS', '')
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
    except:
        dt = ts
    author = r.get('author', 'unknown')
    ch = r.get('channel', '')
    ws = r.get('workspace', '')
    text = r.get('text', '') or r.get('text_snippet', '') or r.get('full_text', '') or r.get('TXT', '') or ''
    prefix = f'#{ch}' if ch else ''
    if ws:
        prefix = f'{ws}  {prefix}' if prefix else ws
    print(f'{dt}  {prefix}  {author}  {text}')
" "$json_data"
}

# ── Installation ──────────────────────────────────────────────────────────────
install_slackdump() {
  header "Installing slackdump"

  if [[ "$OSTYPE" == darwin* ]] && command -v brew >/dev/null 2>&1; then
    info "Installing via Homebrew…"
    brew install slackdump
    return
  fi

  local os arch tag os_cap
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in
    x86_64|amd64) arch="amd64" ;;
    arm64|aarch64) arch="arm64" ;;
    *) die "Unsupported architecture: $arch" ;;
  esac
  case "$os" in
    darwin) os_cap="Darwin" ;;
    linux)  os_cap="Linux" ;;
    *)      die "Unsupported OS: $os" ;;
  esac

  info "Fetching latest slackdump release tag…"
  tag=$(curl -fsSL "https://api.github.com/repos/rusq/slackdump/releases/latest" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['tag_name'])" 2>/dev/null) \
    || die "Could not fetch latest release tag."

  local url tmpdir
  url="https://github.com/rusq/slackdump/releases/download/${tag}/slackdump_${os_cap}_${arch}.tar.gz"
  tmpdir=$(mktemp -d)
  info "Downloading slackdump ${tag} (${os_cap}/${arch})…"
  curl -fsSL "$url" | tar -xz -C "$tmpdir" \
    || die "Download failed: $url"

  local install_dir="${HOME}/.local/bin"
  mkdir -p "$install_dir"
  install "$tmpdir/slackdump" "$install_dir/slackdump"
  rm -rf "$tmpdir"
  export PATH="$install_dir:$PATH"
  info "slackdump installed at ${install_dir}/slackdump"
  warn "Add ${install_dir} to your PATH (e.g. in ~/.zshrc)."
}

ensure_slackdump() {
  if ! command -v slackdump >/dev/null 2>&1; then
    warn "slackdump not found — installing…"
    install_slackdump
  fi
  command -v slackdump >/dev/null 2>&1 \
    || die "slackdump not found. Install manually: https://github.com/rusq/slackdump"
}

ensure_sqlite3() {
  command -v sqlite3 >/dev/null 2>&1 \
    || die "sqlite3 not found. Install: brew install sqlite / apt install sqlite3"
}

ensure_python3() {
  command -v python3 >/dev/null 2>&1 \
    || die "python3 not found."
}

# ── Init ──────────────────────────────────────────────────────────────────────
init_dirs() {
  mkdir -p "$SLACKSLAW_DIR" "$WORKSPACES_DIR"
  [[ -f "$STATE_FILE"  ]] || echo '{}' > "$STATE_FILE"
  [[ -f "$CONFIG_FILE" ]] || echo '{"workspaces":[]}' > "$CONFIG_FILE"
  [[ -f "$LOG_FILE"    ]] || touch "$LOG_FILE"
}

# ── Workspace & DB helpers ────────────────────────────────────────────────────
list_configured_workspaces() {
  json_array_get "$CONFIG_FILE" "workspaces"
}

workspace_dir() {
  # Use short name (without .slack.com) for the directory
  local short="${1%.slack.com}"
  echo "$WORKSPACES_DIR/$short"
}

# slackdump writes the SQLite file inside the output directory.
# The filename varies by version — discover it at runtime.
find_workspace_db() {
  local ws_dir="$1"
  find "$ws_dir" -maxdepth 2 \( -name "*.db" -o -name "*.sqlite" \) 2>/dev/null | sort | head -1
}

record_workspace() {
  json_array_append "$CONFIG_FILE" "workspaces" "$1"
}

# Probe the actual table name for messages or users across slackdump versions
probe_table() {
  local db="$1"; shift
  local candidates=("$@")
  for t in "${candidates[@]}"; do
    local found
    found=$(sqlite3 "$db" \
      "SELECT name FROM sqlite_master WHERE type='table' AND name='${t}' LIMIT 1;" \
      2>/dev/null || true)
    [[ -n "$found" ]] && echo "$t" && return
  done
  echo "${candidates[0]}"  # fallback to first candidate
}

# Probe the text column name in the messages table (TXT in v4, text in v3)
probe_text_col() {
  local db="$1" table="$2"
  local cols
  cols=$(sqlite3 "$db" "PRAGMA table_info(${table});" 2>/dev/null || true)
  if echo "$cols" | grep -qi '|TXT|'; then
    echo "TXT"
  else
    echo "text"
  fi
}

# Probe whether user_id is a direct column or needs json_extract
probe_user_expr() {
  local db="$1" table="$2"
  local cols
  cols=$(sqlite3 "$db" "PRAGMA table_info(${table});" 2>/dev/null || true)
  if echo "$cols" | grep -qi '|user_id|'; then
    echo "m.user_id"
  else
    echo "json_extract(m.DATA, '\$.user')"
  fi
}

# ── Commands ──────────────────────────────────────────────────────────────────

cmd_auth() {
  ensure_slackdump
  init_dirs

  header "Workspace Authentication"

  local ws_hint="${1:-}"
  if [[ -z "$ws_hint" ]]; then
    echo "Enter the workspace name or URL (e.g. myteam or myteam.slack.com):"
    read -r ws_hint
  fi

  # Normalise: strip protocol/path but keep .slack.com (slackdump v4 needs it)
  local ws_name
  ws_name=$(echo "$ws_hint" | sed 's|https\?://||; s|/.*||')
  # Ensure it ends with .slack.com (slackdump v4 uses this as the workspace ID)
  [[ "$ws_name" != *.slack.com ]] && ws_name="${ws_name}.slack.com"
  [[ -z "$ws_name" || "$ws_name" == ".slack.com" ]] && die "Could not determine workspace name from: $ws_hint"

  mkdir -p "$(workspace_dir "$ws_name")"

  info "Initiating browser-based login for: ${BOLD}${ws_name}${RESET}"
  info "A browser window will open — log in as usual."
  echo ""

  # slackdump workspace new handles EZ-Login 3000 and stores credentials in
  # its own config (~/.config/slackdump/ or ~/.slackdump/).
  slackdump workspace new "$ws_hint" \
    || die "Authentication failed for $ws_name"

  record_workspace "$ws_name"

  info "Workspace '${ws_name}' authenticated."
  info "Run: ${BOLD}slackslaw sync --full${RESET} to download the archive."
}

cmd_workspaces() {
  init_dirs
  header "Configured Workspaces"

  local workspaces
  workspaces=$(list_configured_workspaces)

  if [[ -z "$workspaces" ]]; then
    warn "No workspaces configured. Run: slackslaw auth"
    return
  fi

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db last_sync msg_count
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")
    last_sync=$(json_get "$STATE_FILE" "${ws}_last_sync")
    if [[ -n "$db" && -f "$db" ]]; then
      local msg_table
      msg_table=$(probe_table "$db" MESSAGE messages message)
      msg_count=$(sqlite3 "$db" "SELECT COUNT(*) FROM ${msg_table};" 2>/dev/null || echo "?")
      echo -e "  ${BOLD}${ws}${RESET}  ${DIM}${msg_count} messages${RESET}  last sync: ${last_sync:-never}"
    else
      echo -e "  ${BOLD}${ws}${RESET}  ${DIM}(not yet synced)${RESET}  last sync: ${last_sync:-never}"
    fi
  done <<< "$workspaces"
}

cmd_keywords() {
  init_dirs

  local action="${1:-}"
  shift 2>/dev/null || true

  case "$action" in
    add)
      [[ -z "${1:-}" ]] && die "Usage: slackslaw keywords add <word>"
      json_array_append "$CONFIG_FILE" "keywords" "$1"
      info "Added keyword: ${BOLD}$1${RESET}"
      ;;
    remove|rm)
      [[ -z "${1:-}" ]] && die "Usage: slackslaw keywords remove <word>"
      python3 - "$CONFIG_FILE" "$1" <<'PYEOF'
import json, sys
path, word = sys.argv[1], sys.argv[2]
try:
    d = json.load(open(path))
except:
    d = {}
arr = d.get("keywords", [])
arr = [k for k in arr if k != word]
d["keywords"] = arr
json.dump(d, open(path, "w"), indent=2)
PYEOF
      info "Removed keyword: ${BOLD}$1${RESET}"
      ;;
    list|"")
      header "Channel Keywords"
      local kws
      kws=$(json_array_get "$CONFIG_FILE" "keywords")
      if [[ -z "$kws" ]]; then
        info "No keywords configured."
        echo "  Add with: ${BOLD}slackslaw keywords add <word>${RESET}"
        echo ""
        echo "  Keywords work like Slack's \"Channel keywords\" setting."
        echo "  Messages containing these words are included when using ${BOLD}--mine${RESET}."
      else
        while IFS= read -r kw; do
          [[ -z "$kw" ]] && continue
          echo -e "  ${CYAN}•${RESET} $kw"
        done <<< "$kws"
      fi
      ;;
    *)
      die "Usage: slackslaw keywords [list|add <word>|remove <word>]"
      ;;
  esac
}

cmd_sync() {
  ensure_slackdump
  init_dirs

  local full_sync=false
  local target_ws=""
  local channel_spec=""
  local since_duration=""
  local quiet_mode=false

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --full)        full_sync=true ;;
      --workspace)   shift; target_ws="$1" ;;
      --channel|-c)  shift; channel_spec="${1#\#}" ;;
      --since)       shift; since_duration="$1" ;;
      --quiet|-q)    quiet_mode=true ;;
      *)             warn "Unknown sync option: $1" ;;
    esac
    shift
  done

  local workspaces
  if [[ -n "$target_ws" ]]; then
    workspaces="$target_ws"
  else
    workspaces=$(list_configured_workspaces)
  fi
  [[ -z "$workspaces" ]] && die "No workspaces configured. Run: slackslaw auth first."

  # Build time-from flag for --since
  local time_from_flag=""
  if [[ -n "$since_duration" ]]; then
    local since_ts
    since_ts=$(parse_duration_to_ts "$since_duration")
    local since_rfc3339
    since_rfc3339=$(python3 -c "
from datetime import datetime, timezone
print(datetime.fromtimestamp(${since_ts}, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
")
    time_from_flag="-time-from ${since_rfc3339}"
  fi

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    $quiet_mode || header "Syncing workspace: $ws"

    local ws_dir
    ws_dir=$(workspace_dir "$ws")
    mkdir -p "$ws_dir"

    local last_sync
    last_sync=$(json_get "$STATE_FILE" "${ws}_last_sync")

    # Capture time before sync for whatsnew baseline
    local sync_start
    sync_start=$(date -u +%s)

    # Resolve channel name → ID if channel_spec is given
    local channel_id=""
    if [[ -n "$channel_spec" ]]; then
      local db
      db=$(find_workspace_db "$ws_dir")
      if [[ -n "$db" && -f "$db" ]]; then
        local ch_table
        ch_table=$(probe_table "$db" CHANNEL channels)
        channel_id=$(sqlite3 "$db" \
          "SELECT ID FROM ${ch_table} WHERE LOWER(NAME)=LOWER('${channel_spec}') LIMIT 1;" \
          2>/dev/null || true)
      fi
      if [[ -z "$channel_id" ]]; then
        # Try using the spec directly as a channel ID
        channel_id="$channel_spec"
      fi
    fi

    # Build the slackdump command
    local output_redirect
    if $quiet_mode; then
      output_redirect=">> $LOG_FILE 2>&1"
    else
      output_redirect="2>&1 | tee -a $LOG_FILE"
    fi

    if $full_sync || [[ -z "$last_sync" ]]; then
      $quiet_mode || info "Full archive sync for ${ws}…"
      # ─────────────────────────────────────────────────────────────────────
      # slackdump v4: -workspace and -y are per-subcommand flags.
      # They go AFTER the subcommand name, not before it.
      #   slackdump archive -workspace <ws> -y -o <dir>   ← correct (v4)
      #   slackdump -w <ws> archive ...                   ← wrong (causes "flag not defined")
      # ─────────────────────────────────────────────────────────────────────
      local cmd="slackdump archive -workspace \"$ws\" -y -o \"$ws_dir\""
      [[ -n "$time_from_flag" ]] && cmd+=" $time_from_flag"
      [[ -n "$channel_id" ]] && cmd+=" \"$channel_id\""
      eval "$cmd $output_redirect" || {
          $quiet_mode || error "slackdump archive failed for '$ws'"
          $quiet_mode || error "Debug: try running manually:"
          $quiet_mode || error "  slackdump archive -workspace $ws -y -o $ws_dir"
          continue
        }
    else
      $quiet_mode || info "Incremental sync for ${ws} (since ${last_sync})…"
      local cmd="slackdump resume -workspace \"$ws\" \"$ws_dir\""
      [[ -n "$time_from_flag" ]] && cmd+=" $time_from_flag"
      [[ -n "$channel_id" ]] && cmd+=" \"$channel_id\""
      eval "$cmd $output_redirect" || {
          $quiet_mode || warn "resume failed for '$ws' — falling back to full archive…"
          local cmd2="slackdump archive -workspace \"$ws\" -y -o \"$ws_dir\""
          [[ -n "$time_from_flag" ]] && cmd2+=" $time_from_flag"
          [[ -n "$channel_id" ]] && cmd2+=" \"$channel_id\""
          eval "$cmd2 $output_redirect" || {
              $quiet_mode || error "archive also failed for '$ws'"
              continue
            }
        }
    fi

    # Update state — snapshot message count AFTER sync for whatsnew baseline
    local db prev_count=0
    db=$(find_workspace_db "$ws_dir")
    if [[ -n "$db" && -f "$db" ]]; then
      local msg_table
      msg_table=$(probe_table "$db" MESSAGE messages message)
      prev_count=$(sqlite3 "$db" "SELECT COUNT(*) FROM ${msg_table};" 2>/dev/null || echo "0")
    fi

    json_set "$STATE_FILE" "${ws}_last_sync"          "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    json_set "$STATE_FILE" "${ws}_last_sync_ts"       "$sync_start"
    json_set "$STATE_FILE" "${ws}_count_at_last_sync" "$prev_count"

    # In quiet mode, report new message count to stderr
    if $quiet_mode; then
      local old_count
      old_count=$(json_get "$STATE_FILE" "${ws}_count_at_last_sync")
      local new_msgs=$(( prev_count - ${old_count:-0} ))
      (( new_msgs > 0 )) && echo "${ws}: ${new_msgs} new messages" >&2
    else
      info "Sync complete for ${ws}."
    fi
  done <<< "$workspaces"
}

cmd_whatsnew() {
  ensure_sqlite3
  init_dirs

  local as_json=false
  local target_ws=""
  local limit=100
  local mine_filter=false
  local format_mode="default"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --json)        as_json=true ;;
      --slackmd)     format_mode="slackmd" ;;
      --format)      shift;
                     case "$1" in
                       json)    as_json=true ;;
                       slackmd) format_mode="slackmd" ;;
                     esac ;;
      --workspace)   shift; target_ws="$1" ;;
      --limit)       shift; limit="$1" ;;
      --mine|-m)     mine_filter=true ;;
      *)             warn "Unknown option: $1" ;;
    esac
    shift
  done

  local workspaces
  if [[ -n "$target_ws" ]]; then
    workspaces="$target_ws"
  else
    workspaces=$(list_configured_workspaces)
  fi
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  local found=false

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")
    [[ -z "$db" || ! -f "$db" ]] && continue

    local since_ts
    since_ts=$(json_get "$STATE_FILE" "${ws}_last_sync_ts")
    [[ -z "$since_ts" ]] && since_ts=0

    local slack_ts
    slack_ts=$(python3 -c "print('%.6f' % float('$since_ts'))" 2>/dev/null || echo "0.000000")

    local msg_table user_table ch_table
    msg_table=$(probe_table  "$db" MESSAGE messages message)
    user_table=$(probe_table "$db" S_USER users user)
    ch_table=$(probe_table "$db" CHANNEL channels)

    local text_col user_expr
    text_col=$(probe_text_col "$db" "$msg_table")
    user_expr=$(probe_user_expr "$db" "$msg_table")

    local user_cols user_name_expr
    user_cols=$(sqlite3 "$db" "PRAGMA table_info(${user_table});" 2>/dev/null || true)
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_name_expr="COALESCE(u.USERNAME, ${user_expr}, 'unknown')"
    else
      user_name_expr="COALESCE(u.real_name, u.name, ${user_expr}, 'unknown')"
    fi

    # Deduplicate channel/user tables (slackdump v4 stores multiple chunks)
    local ch_dedup="(SELECT ID, NAME FROM ${ch_table} GROUP BY ID)"
    local user_dedup
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_dedup="(SELECT ID, USERNAME FROM ${user_table} GROUP BY ID)"
    else
      user_dedup="(SELECT ID, real_name, name FROM ${user_table} GROUP BY ID)"
    fi

    local query="
SELECT
  m.ts,
  ${user_name_expr} AS author,
  COALESCE(c.NAME, m.CHANNEL_ID, '')                   AS channel,
  m.${text_col},
  '${ws}'                                              AS workspace,
  m.CHANNEL_ID                                         AS channel_id
FROM ${msg_table} m
LEFT JOIN ${ch_dedup} c ON c.ID = m.CHANNEL_ID
LEFT JOIN ${user_dedup} u ON u.ID = ${user_expr}
WHERE CAST(m.ts AS REAL) > ${slack_ts}
  AND (m.${text_col} IS NOT NULL AND m.${text_col} != '')"

    if $mine_filter; then
      local mine_clause
      mine_clause=$(build_mine_clause "$db" "$msg_table" "$text_col" "$user_expr")
      [[ -n "$mine_clause" ]] && query+=" AND ${mine_clause}"
    fi

    query+="
GROUP BY m.TS, m.CHANNEL_ID
ORDER BY CAST(m.ts AS REAL) ASC
LIMIT ${limit};"

    local rows_json
    rows_json=$(sqlite3 -json "$db" "$query" 2>/dev/null || echo "[]")
    if [[ "$rows_json" != "[]" && -n "$rows_json" ]]; then
      found=true
      if $as_json; then
        echo "$rows_json" | python3 -c "
import json,sys,datetime
rows=json.load(sys.stdin)
for r in rows:
    try:
        r['datetime']=datetime.datetime.fromtimestamp(float(r.get('ts') or r.get('TS','')),tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    except: pass
    print(json.dumps(r))
"
      elif [[ "$format_mode" == "slackmd" ]]; then
        format_rows_slackmd "$rows_json"
      else
        echo -e "\n${BOLD}${CYAN}Workspace: ${ws}${RESET}"
        format_rows "$db" "$user_table" "list" "$rows_json"
      fi
    fi
  done <<< "$workspaces"

  if ! $found; then
    if $as_json; then echo "[]"; else info "No new messages since last sync."; fi
  fi
}

cmd_unread() {
  ensure_sqlite3
  init_dirs

  local as_json=false
  local target_ws=""
  local limit=100
  local format_mode="default"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --json)        as_json=true ;;
      --slackmd)     format_mode="slackmd" ;;
      --format)      shift;
                     case "$1" in
                       json)    as_json=true ;;
                       slackmd) format_mode="slackmd" ;;
                     esac ;;
      --workspace)   shift; target_ws="$1" ;;
      --limit)       shift; limit="$1" ;;
      *)             warn "Unknown option: $1" ;;
    esac
    shift
  done

  local workspaces
  if [[ -n "$target_ws" ]]; then
    workspaces="$target_ws"
  else
    workspaces=$(list_configured_workspaces)
  fi
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  local found=false

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")
    [[ -z "$db" || ! -f "$db" ]] && continue

    local msg_table user_table ch_table
    msg_table=$(probe_table  "$db" MESSAGE messages message)
    user_table=$(probe_table "$db" S_USER users user)
    ch_table=$(probe_table "$db" CHANNEL channels)

    local text_col user_expr
    text_col=$(probe_text_col "$db" "$msg_table")
    user_expr=$(probe_user_expr "$db" "$msg_table")

    local user_cols user_name_expr
    user_cols=$(sqlite3 "$db" "PRAGMA table_info(${user_table});" 2>/dev/null || true)
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_name_expr="COALESCE(u.USERNAME, ${user_expr}, 'unknown')"
    else
      user_name_expr="COALESCE(u.real_name, u.name, ${user_expr}, 'unknown')"
    fi

    local ch_dedup="(SELECT ID, NAME, DATA FROM ${ch_table} GROUP BY ID)"
    local user_dedup
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_dedup="(SELECT ID, USERNAME FROM ${user_table} GROUP BY ID)"
    else
      user_dedup="(SELECT ID, real_name, name FROM ${user_table} GROUP BY ID)"
    fi

    # Find messages newer than the channel's last_read timestamp
    local query="
SELECT
  m.ts,
  ${user_name_expr} AS author,
  COALESCE(c.NAME, m.CHANNEL_ID, '')                   AS channel,
  m.${text_col}                                        AS text,
  '${ws}'                                              AS workspace,
  m.CHANNEL_ID                                         AS channel_id
FROM ${msg_table} m
JOIN ${ch_dedup} c ON c.ID = m.CHANNEL_ID
LEFT JOIN ${user_dedup} u ON u.ID = ${user_expr}
WHERE CAST(m.ts AS REAL) > CAST(json_extract(c.DATA, '\$.last_read') AS REAL)
  AND json_extract(c.DATA, '\$.last_read') IS NOT NULL
  AND json_extract(c.DATA, '\$.last_read') != '0000000000.000000'
  AND (m.${text_col} IS NOT NULL AND m.${text_col} != '')
GROUP BY m.TS, m.CHANNEL_ID
ORDER BY CAST(m.ts AS REAL) DESC
LIMIT ${limit};"

    local rows_json
    rows_json=$(sqlite3 -json "$db" "$query" 2>/dev/null || echo "[]")
    if [[ "$rows_json" != "[]" && -n "$rows_json" ]]; then
      found=true
      if $as_json; then
        echo "$rows_json" | python3 -c "
import json,sys,datetime
rows=json.load(sys.stdin)
for r in rows:
    try:
        r['datetime']=datetime.datetime.fromtimestamp(float(r.get('ts') or r.get('TS','')),tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    except: pass
    print(json.dumps(r))
"
      elif [[ "$format_mode" == "slackmd" ]]; then
        format_rows_slackmd "$rows_json"
      else
        echo -e "\n${BOLD}${CYAN}Workspace: ${ws}${RESET}"
        format_rows "$db" "$user_table" "detail" "$rows_json"
      fi
    fi
  done <<< "$workspaces"

  if ! $found; then
    if $as_json; then echo "[]"; else info "No unread messages."; fi
  fi
}

cmd_search() {
  ensure_sqlite3
  init_dirs

  local query=""
  local as_json=false
  local target_ws=""
  local channel_filter=""
  local author_filter=""
  local limit=20
  local mine_filter=false
  local sort_order="ASC"
  local after_ts="" before_ts=""
  local format_mode="default"

  if [[ $# -gt 0 && "${1:0:1}" != "-" ]]; then
    query="$1"; shift
  fi

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --json)            as_json=true ;;
      --slackmd)         format_mode="slackmd" ;;
      --workspace|-w)    shift; target_ws="$1" ;;
      --channel|-c)      shift; channel_filter="${1#\#}" ;;
      --author|-a)       shift; author_filter="$1" ;;
      --limit|-l)        shift; limit="$1" ;;
      --mine|-m)         mine_filter=true ;;
      --newest-first)    sort_order="DESC" ;;
      --after)           shift; after_ts=$(parse_date_to_ts "$1") ;;
      --before)          shift; before_ts=$(parse_date_to_ts "$1") ;;
      --format)          shift;
                         case "$1" in
                           json)    as_json=true ;;
                           slackmd) format_mode="slackmd" ;;
                           *)       format_mode="$1" ;;
                         esac ;;
      *)                 warn "Unknown search option: $1" ;;
    esac
    shift
  done

  [[ -z "$query" ]] && die \
    'Usage: slackslaw search "expr" [--channel C] [--author A] [--mine] [--newest-first] [--workspace W] [--limit N] [--json]'

  local workspaces
  if [[ -n "$target_ws" ]]; then
    workspaces="$target_ws"
  else
    workspaces=$(list_configured_workspaces)
  fi
  [[ -z "$workspaces" ]] && die "No workspaces configured. Run: slackslaw auth"

  local any_results=false
  # Collect all results across workspaces for unified sorting
  local all_results_json="[]"
  local last_db="" last_user_table=""

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")

    if [[ -z "$db" || ! -f "$db" ]]; then
      warn "No archive for workspace '$ws' — run: slackslaw sync --full"
      continue
    fi

    local msg_table user_table
    msg_table=$(probe_table  "$db" MESSAGE messages message)
    user_table=$(probe_table "$db" S_USER users user)
    local ch_table
    ch_table=$(probe_table "$db" CHANNEL channels)

    local text_col user_expr
    text_col=$(probe_text_col "$db" "$msg_table")
    user_expr=$(probe_user_expr "$db" "$msg_table")

    # Detect user display name columns
    local user_name_expr
    local user_cols
    user_cols=$(sqlite3 "$db" "PRAGMA table_info(${user_table});" 2>/dev/null || true)
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_name_expr="COALESCE(u.USERNAME, ${user_expr}, 'unknown')"
    else
      user_name_expr="COALESCE(u.real_name, u.name, ${user_expr}, 'unknown')"
    fi

    # Detect FTS table (slackdump may name it messages_fts, fts_messages, etc.)
    local fts_table
    fts_table=$(sqlite3 "$db" \
      "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%fts%' LIMIT 1;" \
      2>/dev/null || true)

    # Escape single quotes in the query for SQL safety
    local q_escaped
    q_escaped=$(echo "$query" | sed "s/'/''/g")

    # Deduplicate channel/user tables (slackdump v4 stores multiple chunks)
    local ch_dedup="(SELECT ID, NAME FROM ${ch_table} GROUP BY ID)"
    local user_dedup
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_dedup="(SELECT ID, USERNAME FROM ${user_table} GROUP BY ID)"
    else
      user_dedup="(SELECT ID, real_name, name FROM ${user_table} GROUP BY ID)"
    fi

    local sql
    if [[ -n "$fts_table" ]]; then
      sql="
SELECT m.ts,
  ${user_name_expr} AS author,
  COALESCE(c.NAME, m.CHANNEL_ID, '')                   AS channel,
  snippet(${fts_table}, 0, '>>>', '<<<', '...', 20)    AS text_snippet,
  m.${text_col}                                        AS full_text,
  '${ws}'                                              AS workspace,
  m.CHANNEL_ID                                         AS channel_id
FROM ${fts_table}
JOIN ${msg_table} m ON ${fts_table}.rowid = m.rowid
LEFT JOIN ${ch_dedup} c ON c.ID = m.CHANNEL_ID
LEFT JOIN ${user_dedup} u ON u.ID = ${user_expr}
WHERE ${fts_table} MATCH '${q_escaped}'"
    else
      sql="
SELECT m.ts,
  ${user_name_expr} AS author,
  COALESCE(c.NAME, m.CHANNEL_ID, '')                   AS channel,
  m.${text_col} AS text_snippet,
  m.${text_col} AS full_text,
  '${ws}' AS workspace,
  m.CHANNEL_ID AS channel_id
FROM ${msg_table} m
LEFT JOIN ${ch_dedup} c ON c.ID = m.CHANNEL_ID
LEFT JOIN ${user_dedup} u ON u.ID = ${user_expr}
WHERE m.${text_col} LIKE '%${q_escaped}%'"
    fi

    [[ -n "$channel_filter" ]] && \
      sql+=" AND (LOWER(COALESCE(c.NAME,'')) = LOWER('${channel_filter}') OR m.CHANNEL_ID = '${channel_filter}')"
    [[ -n "$author_filter" ]] && \
      sql+=" AND (LOWER(COALESCE(u.USERNAME,'')) LIKE LOWER('%${author_filter}%'))"

    if $mine_filter; then
      local mine_clause
      mine_clause=$(build_mine_clause "$db" "$msg_table" "$text_col" "$user_expr")
      [[ -n "$mine_clause" ]] && sql+=" AND ${mine_clause}"
    fi

    [[ -n "$after_ts" ]] && sql+=" AND CAST(m.ts AS REAL) > ${after_ts}"
    [[ -n "$before_ts" ]] && sql+=" AND CAST(m.ts AS REAL) < ${before_ts}"

    sql+=" GROUP BY m.TS, m.CHANNEL_ID ORDER BY CAST(m.ts AS REAL) ${sort_order} LIMIT ${limit};"

    local rows_json
    rows_json=$(sqlite3 -json "$db" "$sql" 2>/dev/null || echo "[]")
    if [[ "$rows_json" != "[]" && -n "$rows_json" ]]; then
      any_results=true
      last_db="$db"
      last_user_table="$user_table"
      # Merge results for cross-workspace unified sorting
      all_results_json=$(python3 -c "
import json, sys
existing = json.loads(sys.argv[1])
new = json.loads(sys.argv[2])
print(json.dumps(existing + new))
" "$all_results_json" "$rows_json")
    fi

  done <<< "$workspaces"

  if ! $any_results; then
    if $as_json; then echo "[]"; else info "No results for: ${query}"; fi
    return
  fi

  # Sort all results globally by timestamp and apply limit
  local sorted_json
  sorted_json=$(python3 -c "
import json, sys
rows = json.loads(sys.argv[1])
reverse = sys.argv[2] == 'DESC'
limit = int(sys.argv[3])
rows.sort(key=lambda r: float(r.get('ts', 0) or r.get('TS', 0) or 0), reverse=reverse)
print(json.dumps(rows[:limit]))
" "$all_results_json" "$sort_order" "$limit")

  if $as_json; then
    echo "$sorted_json" | python3 -c "
import json,sys,datetime
rows=json.load(sys.stdin)
for r in rows:
    try:
        r['datetime']=datetime.datetime.fromtimestamp(float(r.get('ts') or r.get('TS','')),tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    except: pass
    print(json.dumps(r))
"
  elif [[ "$format_mode" == "slackmd" ]]; then
    echo "$sorted_json" | python3 -c "
import json, sys
from datetime import datetime, timezone
rows = json.loads(sys.stdin.read())
for r in rows:
    ts = r.get('ts', '') or r.get('TS', '')
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
    except:
        dt = ts
    author = r.get('author', 'unknown')
    ch = r.get('channel', '')
    ws = r.get('workspace', '')
    text = r.get('text_snippet', '') or r.get('full_text', '') or r.get('TXT', '') or ''
    print(f'{dt}  #{ch}  {author}  {text}')
"
  else
    format_rows "$last_db" "$last_user_table" "detail" "$sorted_json"
  fi
}

cmd_sql() {
  ensure_sqlite3
  init_dirs

  local raw_sql="${1:-}"
  local target_ws="${2:-}"

  [[ "$raw_sql" == "-" ]] && raw_sql=$(cat)
  [[ -z "$raw_sql" ]] && die "Usage: slackslaw sql 'SELECT ...' [workspace]
  Or:    echo 'SELECT ...' | slackslaw sql -"

  local workspaces
  if [[ -n "$target_ws" ]]; then
    workspaces="$target_ws"
  else
    workspaces=$(list_configured_workspaces)
  fi

  local ws_count
  ws_count=$(echo "$workspaces" | grep -c . 2>/dev/null || echo "1")

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")

    if [[ -z "$db" || ! -f "$db" ]]; then
      warn "No DB for workspace '$ws' — run: slackslaw sync --full"
      continue
    fi

    [[ "$ws_count" -gt 1 ]] && echo -e "\n${BOLD}${CYAN}Workspace: ${ws}${RESET}"
    sqlite3 -column -header "$db" "$raw_sql" 2>&1 || warn "SQL error in workspace: $ws"
  done <<< "$workspaces"
}

cmd_status() {
  ensure_sqlite3
  init_dirs
  header "Slack Archive Status"

  local workspaces
  workspaces=$(list_configured_workspaces)

  if [[ -z "$workspaces" ]]; then
    warn "No workspaces configured. Run: slackslaw auth"
    return
  fi

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db last_sync
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")
    last_sync=$(json_get "$STATE_FILE" "${ws}_last_sync")

    echo -e "\n${BOLD}${ws}${RESET}"

    if [[ -z "$db" || ! -f "$db" ]]; then
      echo -e "  ${YELLOW}Not yet synced.${RESET} Run: slackslaw sync --full"
      continue
    fi

    local msg_table user_table ch_table
    msg_table=$(probe_table  "$db" MESSAGE messages message)
    user_table=$(probe_table "$db" S_USER users user)
    ch_table=$(probe_table "$db" CHANNEL channels)

    local msg_count ch_count user_count db_size
    db_size=$(du -sh "$db" 2>/dev/null | cut -f1 || echo "?")
    msg_count=$(sqlite3  "$db" "SELECT COUNT(*) FROM ${msg_table};"  2>/dev/null || echo "?")
    ch_count=$(sqlite3   "$db" "SELECT COUNT(*) FROM ${ch_table};"    2>/dev/null || echo "?")
    user_count=$(sqlite3 "$db" "SELECT COUNT(*) FROM ${user_table};"  2>/dev/null || echo "?")

    echo -e "  Messages : ${BOLD}${msg_count}${RESET}"
    echo -e "  Channels : ${ch_count}"
    echo -e "  Users    : ${user_count}"
    echo -e "  DB size  : ${db_size}"
    echo -e "  Last sync: ${last_sync:-never}"
    echo -e "  DB path  : ${DIM}${db}${RESET}"
  done <<< "$workspaces"
}

cmd_doctor() {
  header "slackslaw Doctor"
  local ok=true

  chk() {
    local label="$1"; shift
    if eval "$*" >/dev/null 2>&1; then
      echo -e "  ${GREEN}✓${RESET} ${label}"
    else
      echo -e "  ${RED}✗${RESET}  ${label}"
      ok=false
    fi
  }

  chk "slackdump installed"   "command -v slackdump"
  chk "sqlite3 installed"     "command -v sqlite3"
  chk "python3 installed"     "command -v python3"
  chk "curl installed"        "command -v curl"
  chk "Config dir exists"     "test -d '$SLACKSLAW_DIR'"
  chk "State file exists"     "test -f '$STATE_FILE'"
  chk "Config file exists"    "test -f '$CONFIG_FILE'"

  echo ""
  local workspaces
  workspaces=$(list_configured_workspaces 2>/dev/null || true)
  if [[ -z "$workspaces" ]]; then
    warn "No workspaces configured — run: slackslaw auth"
  else
    while IFS= read -r ws; do
      [[ -z "$ws" ]] && continue
      local ws_dir db
      ws_dir=$(workspace_dir "$ws")
      db=$(find_workspace_db "$ws_dir")
      if [[ -n "$db" && -f "$db" ]]; then
        echo -e "  ${GREEN}✓${RESET} Archive DB for '${ws}' → ${DIM}${db}${RESET}"
      else
        echo -e "  ${RED}✗${RESET}  No archive DB for '${ws}' — run: slackslaw sync --full"
        ok=false
      fi
    done <<< "$workspaces"
  fi

  echo ""
  if $ok; then info "All checks passed."; else warn "Some checks failed."; fi

  echo -e "\n${DIM}Config dir : $SLACKSLAW_DIR"
  echo -e "State file : $STATE_FILE"
  echo -e "Log file   : $LOG_FILE${RESET}"

  if command -v slackdump >/dev/null 2>&1; then
    echo ""
    slackdump --version 2>/dev/null || slackdump version 2>/dev/null || true
  fi
}

cmd_stats() {
  ensure_sqlite3
  init_dirs

  local workspaces
  workspaces=$(list_configured_workspaces)
  [[ -z "$workspaces" ]] && die "No workspaces configured. Run: slackslaw auth"

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")
    [[ -z "$db" || ! -f "$db" ]] && continue

    local msg_table user_table ch_table
    msg_table=$(probe_table  "$db" MESSAGE messages message)
    user_table=$(probe_table "$db" S_USER users user)
    ch_table=$(probe_table   "$db" CHANNEL channels)

    local text_col user_expr
    text_col=$(probe_text_col "$db" "$msg_table")
    user_expr=$(probe_user_expr "$db" "$msg_table")

    local user_cols
    user_cols=$(sqlite3 "$db" "PRAGMA table_info(${user_table});" 2>/dev/null || true)
    local user_name_expr
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_name_expr="COALESCE(u.USERNAME, ${user_expr}, 'unknown')"
    else
      user_name_expr="COALESCE(u.real_name, u.name, ${user_expr}, 'unknown')"
    fi
    local user_dedup
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_dedup="(SELECT ID, USERNAME FROM ${user_table} GROUP BY ID)"
    else
      user_dedup="(SELECT ID, real_name, name FROM ${user_table} GROUP BY ID)"
    fi
    local ch_dedup="(SELECT ID, NAME FROM ${ch_table} GROUP BY ID)"

    header "Stats: ${ws}"

    # Totals
    local msg_count ch_count user_count
    msg_count=$(sqlite3 "$db" "SELECT COUNT(*) FROM ${msg_table};" 2>/dev/null || echo "0")
    ch_count=$(sqlite3 "$db" "SELECT COUNT(DISTINCT ID) FROM ${ch_table};" 2>/dev/null || echo "0")
    user_count=$(sqlite3 "$db" "SELECT COUNT(DISTINCT ID) FROM ${user_table};" 2>/dev/null || echo "0")
    local date_range
    date_range=$(sqlite3 "$db" "
      SELECT MIN(datetime(CAST(ts AS REAL), 'unixepoch')) || ' → ' || MAX(datetime(CAST(ts AS REAL), 'unixepoch'))
      FROM ${msg_table} WHERE ts IS NOT NULL;" 2>/dev/null || echo "?")

    echo -e "  Total messages : ${BOLD}${msg_count}${RESET}"
    echo -e "  Channels       : ${ch_count}"
    echo -e "  Users          : ${user_count}"
    echo -e "  Date range     : ${date_range}"

    # Top 10 senders
    echo -e "\n  ${BOLD}Top 10 Message Senders${RESET}"
    sqlite3 "$db" "
      SELECT ${user_name_expr} AS author, COUNT(*) AS cnt
      FROM ${msg_table} m
      LEFT JOIN ${user_dedup} u ON u.ID = ${user_expr}
      WHERE m.${text_col} IS NOT NULL AND m.${text_col} != ''
      GROUP BY ${user_expr}
      ORDER BY cnt DESC
      LIMIT 10;
    " 2>/dev/null | while IFS='|' read -r author cnt; do
      printf "    ${GREEN}%-20s${RESET} %s messages\n" "$author" "$cnt"
    done

    # Top 10 busiest channels
    echo -e "\n  ${BOLD}Top 10 Busiest Channels${RESET}"
    sqlite3 "$db" "
      SELECT COALESCE(c.NAME, m.CHANNEL_ID) AS channel, COUNT(*) AS cnt
      FROM ${msg_table} m
      LEFT JOIN ${ch_dedup} c ON c.ID = m.CHANNEL_ID
      WHERE m.${text_col} IS NOT NULL AND m.${text_col} != ''
      GROUP BY m.CHANNEL_ID
      ORDER BY cnt DESC
      LIMIT 10;
    " 2>/dev/null | while IFS='|' read -r channel cnt; do
      printf "    ${CYAN}#%-20s${RESET} %s messages\n" "$channel" "$cnt"
    done

    # Most active hours
    echo -e "\n  ${BOLD}Most Active Hours (UTC)${RESET}"
    python3 - "$db" "$msg_table" <<'PYEOF'
import sqlite3, sys
db_path, msg_table = sys.argv[1], sys.argv[2]
conn = sqlite3.connect(db_path)
rows = conn.execute(f"""
    SELECT CAST(strftime('%H', datetime(CAST(ts AS REAL), 'unixepoch')) AS INTEGER) AS hour, COUNT(*) AS cnt
    FROM {msg_table}
    WHERE ts IS NOT NULL
    GROUP BY hour
    ORDER BY hour;
""").fetchall()
conn.close()
if not rows:
    sys.exit(0)
max_cnt = max(r[1] for r in rows)
for hour, cnt in rows:
    bar_len = int(cnt / max_cnt * 30) if max_cnt > 0 else 0
    bar = '█' * bar_len
    print(f"    {hour:02d}:00  {bar} {cnt}")
PYEOF

  done <<< "$workspaces"
}

cmd_threads() {
  ensure_sqlite3
  init_dirs

  local url="${1:-}"
  shift 2>/dev/null || true

  local as_json=false
  local format_mode="default"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --json)   as_json=true ;;
      --slackmd) format_mode="slackmd" ;;
      --format) shift;
                case "$1" in
                  json)    as_json=true ;;
                  slackmd) format_mode="slackmd" ;;
                esac ;;
      *)        warn "Unknown option: $1" ;;
    esac
    shift
  done

  [[ -z "$url" ]] && die "Usage: slackslaw threads <slack-url> [--json]
  Example: slackslaw threads https://myteam.slack.com/archives/C0123ABC/p1234567890123456"

  # Parse URL: https://workspace.slack.com/archives/CXXX/pTTTTTTTTTTTTTTTT
  local channel_id thread_ts
  if [[ "$url" =~ /archives/([A-Z0-9]+)/p([0-9]+) ]]; then
    channel_id="${BASH_REMATCH[1]}"
    local raw_ts="${BASH_REMATCH[2]}"
    # Convert p-prefixed timestamp: insert '.' before last 6 digits
    local len=${#raw_ts}
    if (( len > 6 )); then
      thread_ts="${raw_ts:0:len-6}.${raw_ts:len-6}"
    else
      thread_ts="$raw_ts"
    fi
  else
    die "Could not parse Slack URL: $url
  Expected format: https://workspace.slack.com/archives/CXXX/pTTTTTTTTTTTTTTTT"
  fi

  local workspaces
  workspaces=$(list_configured_workspaces)
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  local found=false

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")
    [[ -z "$db" || ! -f "$db" ]] && continue

    local msg_table user_table ch_table
    msg_table=$(probe_table  "$db" MESSAGE messages message)
    user_table=$(probe_table "$db" S_USER users user)
    ch_table=$(probe_table   "$db" CHANNEL channels)

    local text_col user_expr
    text_col=$(probe_text_col "$db" "$msg_table")
    user_expr=$(probe_user_expr "$db" "$msg_table")

    local user_cols user_name_expr
    user_cols=$(sqlite3 "$db" "PRAGMA table_info(${user_table});" 2>/dev/null || true)
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_name_expr="COALESCE(u.USERNAME, ${user_expr}, 'unknown')"
    else
      user_name_expr="COALESCE(u.real_name, u.name, ${user_expr}, 'unknown')"
    fi
    local ch_dedup="(SELECT ID, NAME FROM ${ch_table} GROUP BY ID)"
    local user_dedup
    if echo "$user_cols" | grep -qi '|USERNAME|'; then
      user_dedup="(SELECT ID, USERNAME FROM ${user_table} GROUP BY ID)"
    else
      user_dedup="(SELECT ID, real_name, name FROM ${user_table} GROUP BY ID)"
    fi

    # Check if this channel exists in this workspace
    local ch_exists
    ch_exists=$(sqlite3 "$db" "SELECT 1 FROM ${ch_table} WHERE ID='${channel_id}' LIMIT 1;" 2>/dev/null || true)
    [[ -z "$ch_exists" ]] && continue

    local sql="
SELECT m.ts,
  ${user_name_expr} AS author,
  COALESCE(c.NAME, m.CHANNEL_ID, '') AS channel,
  m.${text_col} AS text,
  '${ws}' AS workspace,
  m.CHANNEL_ID AS channel_id
FROM ${msg_table} m
LEFT JOIN ${ch_dedup} c ON c.ID = m.CHANNEL_ID
LEFT JOIN ${user_dedup} u ON u.ID = ${user_expr}
WHERE m.CHANNEL_ID = '${channel_id}'
  AND (m.THREAD_TS = '${thread_ts}' OR m.TS = '${thread_ts}')
ORDER BY CAST(m.ts AS REAL) ASC;"

    local rows_json
    rows_json=$(sqlite3 -json "$db" "$sql" 2>/dev/null || echo "[]")
    if [[ "$rows_json" != "[]" && -n "$rows_json" ]]; then
      found=true
      if $as_json; then
        echo "$rows_json" | python3 -c "
import json,sys,datetime
rows=json.load(sys.stdin)
for r in rows:
    try:
        r['datetime']=datetime.datetime.fromtimestamp(float(r.get('ts') or r.get('TS','')),tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    except: pass
    print(json.dumps(r))
"
      elif [[ "$format_mode" == "slackmd" ]]; then
        format_rows_slackmd "$rows_json"
      else
        header "Thread in #$(sqlite3 "$db" "SELECT NAME FROM ${ch_table} WHERE ID='${channel_id}' LIMIT 1;" 2>/dev/null || echo "$channel_id") (${ws})"
        format_rows "$db" "$user_table" "detail" "$rows_json"
      fi
    fi
  done <<< "$workspaces"

  if ! $found; then
    if $as_json; then echo "[]"; else info "Thread not found in any workspace."; fi
  fi
}

cmd_tail() {
  ensure_slackdump
  ensure_sqlite3
  init_dirs

  local interval=30
  local as_json=false
  local mine_filter=false
  local channel_filter=""
  local target_ws=""
  local format_mode="default"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -f|--follow)   ;; # accepted for familiar `tail -f` usage
      --interval)    shift; interval="$1" ;;
      --json)        as_json=true ;;
      --slackmd)     format_mode="slackmd" ;;
      --format)      shift;
                     case "$1" in
                       json)    as_json=true ;;
                       slackmd) format_mode="slackmd" ;;
                     esac ;;
      --mine|-m)     mine_filter=true ;;
      --channel|-c)  shift; channel_filter="${1#\#}" ;;
      --workspace|-w) shift; target_ws="$1" ;;
      *)             warn "Unknown tail option: $1" ;;
    esac
    shift
  done

  local workspaces
  if [[ -n "$target_ws" ]]; then
    workspaces="$target_ws"
  else
    workspaces=$(list_configured_workspaces)
  fi
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  info "Watching for new messages (poll every ${interval}s, Ctrl-C to stop)…"

  # Track last seen timestamp per workspace (file-based for bash 3 compat)
  local tail_state_dir
  tail_state_dir=$(mktemp -d)
  local init_ts
  init_ts=$(date -u +%s)
  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    echo "$init_ts" > "${tail_state_dir}/${ws}"
  done <<< "$workspaces"

  trap 'echo ""; rm -rf "$tail_state_dir"; info "Stopped."; exit 0' INT

  while true; do
    # Sync all workspaces
    while IFS= read -r ws; do
      [[ -z "$ws" ]] && continue
      local ws_dir
      ws_dir=$(workspace_dir "$ws")
      [[ ! -d "$ws_dir" ]] && continue

      # Incremental sync (quiet)
      slackdump resume -workspace "$ws" "$ws_dir" >> "$LOG_FILE" 2>&1 || true

      local db
      db=$(find_workspace_db "$ws_dir")
      [[ -z "$db" || ! -f "$db" ]] && continue

      local msg_table user_table ch_table
      msg_table=$(probe_table  "$db" MESSAGE messages message)
      user_table=$(probe_table "$db" S_USER users user)
      ch_table=$(probe_table   "$db" CHANNEL channels)

      local text_col user_expr
      text_col=$(probe_text_col "$db" "$msg_table")
      user_expr=$(probe_user_expr "$db" "$msg_table")

      local user_cols user_name_expr
      user_cols=$(sqlite3 "$db" "PRAGMA table_info(${user_table});" 2>/dev/null || true)
      if echo "$user_cols" | grep -qi '|USERNAME|'; then
        user_name_expr="COALESCE(u.USERNAME, ${user_expr}, 'unknown')"
      else
        user_name_expr="COALESCE(u.real_name, u.name, ${user_expr}, 'unknown')"
      fi
      local ch_dedup="(SELECT ID, NAME FROM ${ch_table} GROUP BY ID)"
      local user_dedup
      if echo "$user_cols" | grep -qi '|USERNAME|'; then
        user_dedup="(SELECT ID, USERNAME FROM ${user_table} GROUP BY ID)"
      else
        user_dedup="(SELECT ID, real_name, name FROM ${user_table} GROUP BY ID)"
      fi

      local since
      since=$(cat "${tail_state_dir}/${ws}" 2>/dev/null || echo "$init_ts")
      local slack_ts
      slack_ts=$(python3 -c "print('%.6f' % float('$since'))" 2>/dev/null || echo "0.000000")

      local sql="
SELECT m.ts,
  ${user_name_expr} AS author,
  COALESCE(c.NAME, m.CHANNEL_ID, '') AS channel,
  m.${text_col} AS text,
  '${ws}' AS workspace,
  m.CHANNEL_ID AS channel_id
FROM ${msg_table} m
LEFT JOIN ${ch_dedup} c ON c.ID = m.CHANNEL_ID
LEFT JOIN ${user_dedup} u ON u.ID = ${user_expr}
WHERE CAST(m.ts AS REAL) > ${slack_ts}
  AND (m.${text_col} IS NOT NULL AND m.${text_col} != '')"

      [[ -n "$channel_filter" ]] && \
        sql+=" AND (LOWER(COALESCE(c.NAME,'')) = LOWER('${channel_filter}') OR m.CHANNEL_ID = '${channel_filter}')"

      if $mine_filter; then
        local mine_clause
        mine_clause=$(build_mine_clause "$db" "$msg_table" "$text_col" "$user_expr")
        [[ -n "$mine_clause" ]] && sql+=" AND ${mine_clause}"
      fi

      sql+=" GROUP BY m.TS, m.CHANNEL_ID ORDER BY CAST(m.ts AS REAL) ASC;"

      local rows_json
      rows_json=$(sqlite3 -json "$db" "$sql" 2>/dev/null || echo "[]")
      if [[ "$rows_json" != "[]" && -n "$rows_json" ]]; then
        if $as_json; then
          echo "$rows_json" | python3 -c "
import json,sys,datetime
rows=json.load(sys.stdin)
for r in rows:
    try:
        r['datetime']=datetime.datetime.fromtimestamp(float(r.get('ts') or r.get('TS','')),tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    except: pass
    print(json.dumps(r))
"
        elif [[ "$format_mode" == "slackmd" ]]; then
          format_rows_slackmd "$rows_json"
        else
          format_rows "$db" "$user_table" "detail" "$rows_json"
        fi

        # Update last_seen_ts to max ts in results
        local max_ts
        max_ts=$(echo "$rows_json" | python3 -c "
import json, sys
rows = json.load(sys.stdin)
print(max(float(r.get('ts', 0) or r.get('TS', 0) or 0) for r in rows))
" 2>/dev/null || echo "$since")
        echo "$max_ts" > "${tail_state_dir}/${ws}"
      fi
    done <<< "$workspaces"

    sleep "$interval"
  done
}

cmd_context() {
  ensure_sqlite3
  init_dirs

  local since_duration="7d"
  local mine_filter=false
  local channel_filter=""
  local target_ws=""
  local limit=1000

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --since)       shift; since_duration="$1" ;;
      --mine|-m)     mine_filter=true ;;
      --channel|-c)  shift; channel_filter="${1#\#}" ;;
      --workspace|-w) shift; target_ws="$1" ;;
      --limit|-l)    shift; limit="$1" ;;
      *)             warn "Unknown context option: $1" ;;
    esac
    shift
  done

  local since_ts
  since_ts=$(parse_duration_to_ts "$since_duration")

  local workspaces
  if [[ -n "$target_ws" ]]; then
    workspaces="$target_ws"
  else
    workspaces=$(list_configured_workspaces)
  fi
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  # Build JSON output grouped by workspace and channel
  python3 -c "
import json, sqlite3, sys, re, os
from datetime import datetime, timezone

since_ts = float(sys.argv[1])
limit = int(sys.argv[2])
mine_filter = sys.argv[3] == 'true'
channel_filter = sys.argv[4]
workspaces_str = sys.argv[5]
workspaces_dir = sys.argv[6]

def prettify_text(text, user_map):
    if not text:
        return ''
    def repl_mention(m):
        return '@' + user_map.get(m.group(1), m.group(1))
    text = re.sub(r'<@([A-Z0-9]+)(?:\|[^>]*)?>', repl_mention, text)
    text = re.sub(r'<mailto:([^|>]+)\|([^>]+)>', r'\2', text)
    text = re.sub(r'<(https?://[^|>]+)\|([^>]+)>', r'\2', text)
    text = re.sub(r'<(https?://[^>]+)>', r'\1', text)
    text = re.sub(r'<#[A-Z0-9]+\|([^>]+)>', r'#\1', text)
    text = re.sub(r'<!([a-z]+)(?:\|[^>]*)?>', r'@\1', text)
    return text

result = []

for ws in workspaces_str.strip().split('\n'):
    ws = ws.strip()
    if not ws:
        continue
    short = ws.replace('.slack.com', '')
    ws_dir = os.path.join(workspaces_dir, short)
    # Find DB
    db_path = None
    for root, dirs, files in os.walk(ws_dir):
        for f in files:
            if f.endswith('.db') or f.endswith('.sqlite'):
                db_path = os.path.join(root, f)
                break
        if db_path:
            break
    if not db_path or not os.path.isfile(db_path):
        continue

    conn = sqlite3.connect(db_path)

    # Probe tables
    tables = [r[0] for r in conn.execute(\"\"\"SELECT name FROM sqlite_master WHERE type='table'\"\"\").fetchall()]
    msg_table = 'MESSAGE' if 'MESSAGE' in tables else 'messages'
    user_table = 'S_USER' if 'S_USER' in tables else 'users'
    ch_table = 'CHANNEL' if 'CHANNEL' in tables else 'channels'

    # Probe columns
    cols = [r[1] for r in conn.execute(f'PRAGMA table_info({msg_table})').fetchall()]
    text_col = 'TXT' if 'TXT' in cols else 'text'

    user_cols = [r[1] for r in conn.execute(f'PRAGMA table_info({user_table})').fetchall()]
    has_username = 'USERNAME' in user_cols

    # Build user map
    user_map = {}
    try:
        if has_username:
            for r in conn.execute(f'SELECT ID, USERNAME FROM {user_table} GROUP BY ID'):
                user_map[r[0]] = r[1]
        else:
            for r in conn.execute(f'SELECT ID, COALESCE(real_name, name) FROM {user_table} GROUP BY ID'):
                user_map[r[0]] = r[1]
    except:
        pass

    # Build channel map
    ch_map = {}
    try:
        for r in conn.execute(f'SELECT ID, NAME FROM {ch_table} GROUP BY ID'):
            ch_map[r[0]] = r[1] or r[0]
    except:
        pass

    # Query messages
    sql = f\"\"\"
        SELECT m.ts, json_extract(m.DATA, '$.user') AS uid, m.CHANNEL_ID, m.{text_col}
        FROM {msg_table} m
        WHERE CAST(m.ts AS REAL) > {since_ts}
          AND m.{text_col} IS NOT NULL AND m.{text_col} != ''
    \"\"\"
    if channel_filter:
        sql += f\" AND (m.CHANNEL_ID IN (SELECT ID FROM {ch_table} WHERE LOWER(NAME) = LOWER('{channel_filter}')) OR m.CHANNEL_ID = '{channel_filter}')\"
    sql += f' ORDER BY CAST(m.ts AS REAL) ASC LIMIT {limit}'

    rows = conn.execute(sql).fetchall()
    conn.close()

    # Group by channel
    channels = {}
    for ts, uid, ch_id, text in rows:
        ch_name = ch_map.get(ch_id, ch_id)
        if ch_name not in channels:
            channels[ch_name] = []
        try:
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        except:
            dt = str(ts)
        channels[ch_name].append({
            'author': user_map.get(uid, uid or 'unknown'),
            'time': dt,
            'text': prettify_text(text, user_map)
        })

    ws_data = {'workspace': ws, 'channels': []}
    for ch_name, msgs in sorted(channels.items()):
        ws_data['channels'].append({'name': '#' + ch_name, 'messages': msgs})
    result.append(ws_data)

print(json.dumps(result, indent=2))
" "$since_ts" "$limit" "$mine_filter" "$channel_filter" "$workspaces" "$WORKSPACES_DIR"
}

cmd_ask() {
  init_dirs

  local question="${1:-}"
  shift 2>/dev/null || true

  local since="7d"
  local provider="anthropic"
  local target_ws=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --since)       shift; since="$1" ;;
      --provider)    shift; provider="$1" ;;
      --workspace|-w) shift; target_ws="$1" ;;
      *)             warn "Unknown ask option: $1" ;;
    esac
    shift
  done

  [[ -z "$question" ]] && die "Usage: slackslaw ask \"question\" [--since 7d] [--provider anthropic|openai]"

  # Get context
  local context_args=(--since "$since")
  [[ -n "$target_ws" ]] && context_args+=(--workspace "$target_ws")
  local context
  context=$(cmd_context "${context_args[@]}")

  local system_prompt="You are a helpful assistant analyzing Slack messages. Below is a JSON dump of recent Slack conversations. Answer the user's question based on this context.

Context:
${context}"

  if [[ "$provider" == "openai" ]]; then
    [[ -z "${OPENAI_API_KEY:-}" ]] && die "OPENAI_API_KEY not set."
    curl -sS https://api.openai.com/v1/chat/completions \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${OPENAI_API_KEY}" \
      -d "$(python3 -c "
import json, sys
print(json.dumps({
    'model': 'gpt-4o',
    'messages': [
        {'role': 'system', 'content': sys.argv[1]},
        {'role': 'user', 'content': sys.argv[2]}
    ]
}))
" "$system_prompt" "$question")" | python3 -c "
import json, sys
resp = json.load(sys.stdin)
if 'choices' in resp:
    print(resp['choices'][0]['message']['content'])
elif 'error' in resp:
    print('Error: ' + resp['error'].get('message', str(resp['error'])), file=sys.stderr)
    sys.exit(1)
"
  elif [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
    curl -sS https://api.anthropic.com/v1/messages \
      -H "Content-Type: application/json" \
      -H "x-api-key: ${ANTHROPIC_API_KEY}" \
      -H "anthropic-version: 2023-06-01" \
      -d "$(python3 -c "
import json, sys
print(json.dumps({
    'model': 'claude-sonnet-4-20250514',
    'max_tokens': 4096,
    'system': sys.argv[1],
    'messages': [
        {'role': 'user', 'content': sys.argv[2]}
    ]
}))
" "$system_prompt" "$question")" | python3 -c "
import json, sys
resp = json.load(sys.stdin)
if 'content' in resp:
    for block in resp['content']:
        if block.get('type') == 'text':
            print(block['text'])
elif 'error' in resp:
    print('Error: ' + resp['error'].get('message', str(resp['error'])), file=sys.stderr)
    sys.exit(1)
"
  else
    # No API key — fall back to claude CLI
    if ! command -v claude &>/dev/null; then
      die "ANTHROPIC_API_KEY not set and 'claude' CLI not found. Set ANTHROPIC_API_KEY or install Claude Code."
    fi
    local prompt
    prompt="${system_prompt}

Question: ${question}"
    claude -p "$prompt"
  fi
}

cmd_mcp() {
  ensure_slackdump
  init_dirs

  local transport="stdio"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --transport) shift; transport="$1" ;;
      *)           warn "Unknown mcp option: $1" ;;
    esac
    shift
  done

  # Find all workspace DBs and pass them
  local workspaces
  workspaces=$(list_configured_workspaces)
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  local ws
  ws=$(echo "$workspaces" | head -1)
  local ws_dir
  ws_dir=$(workspace_dir "$ws")

  info "Starting MCP server (transport: ${transport})…"
  slackdump mcp -workspace "$ws" -transport "$transport"
}

cmd_export() {
  ensure_sqlite3
  init_dirs

  local channel_filter=""
  local format="md"
  local output_file=""
  local target_ws=""
  local deeplinks="1"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --channel|-c)     shift; channel_filter="${1#\#}" ;;
      --format)         shift; format="$1" ;;
      --output|-o)      shift; output_file="$1" ;;
      --workspace|-w)   shift; target_ws="$1" ;;
      --no-deeplinks)   deeplinks="0" ;;
      *)                warn "Unknown export option: $1" ;;
    esac
    shift
  done

  [[ -z "$channel_filter" ]] && die "Usage: slackslaw export --channel \"#name\" [--format md|html] [--output file] [--workspace W] [--no-deeplinks]"

  local workspaces
  if [[ -n "$target_ws" ]]; then
    workspaces="$target_ws"
  else
    workspaces=$(list_configured_workspaces)
  fi
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  local output_func
  if [[ -n "$output_file" ]]; then
    output_func() { cat > "$output_file"; }
  else
    output_func() { cat; }
  fi

  {
    while IFS= read -r ws; do
      [[ -z "$ws" ]] && continue
      local ws_dir db
      ws_dir=$(workspace_dir "$ws")
      db=$(find_workspace_db "$ws_dir")
      [[ -z "$db" || ! -f "$db" ]] && continue

      local msg_table user_table ch_table
      msg_table=$(probe_table  "$db" MESSAGE messages message)
      user_table=$(probe_table "$db" S_USER users user)
      ch_table=$(probe_table   "$db" CHANNEL channels)

      local text_col user_expr
      text_col=$(probe_text_col "$db" "$msg_table")
      user_expr=$(probe_user_expr "$db" "$msg_table")

      local user_cols user_name_expr
      user_cols=$(sqlite3 "$db" "PRAGMA table_info(${user_table});" 2>/dev/null || true)
      if echo "$user_cols" | grep -qi '|USERNAME|'; then
        user_name_expr="COALESCE(u.USERNAME, ${user_expr}, 'unknown')"
      else
        user_name_expr="COALESCE(u.real_name, u.name, ${user_expr}, 'unknown')"
      fi
      local user_dedup
      if echo "$user_cols" | grep -qi '|USERNAME|'; then
        user_dedup="(SELECT ID, USERNAME FROM ${user_table} GROUP BY ID)"
      else
        user_dedup="(SELECT ID, real_name, name FROM ${user_table} GROUP BY ID)"
      fi
      local ch_dedup="(SELECT ID, NAME FROM ${ch_table} GROUP BY ID)"

      local rows_json
      rows_json=$(sqlite3 -json "$db" "
        SELECT m.TS AS ts,
          ${user_name_expr} AS author,
          COALESCE(c.NAME, m.CHANNEL_ID, '') AS channel,
          MAX(m.${text_col}) AS text,
          m.CHANNEL_ID AS channel_id,
          MAX(m.THREAD_TS) AS thread_ts,
          ${user_expr} AS user_id,
          MAX(json_extract(m.DATA, '\$.files')) AS files,
          MAX(json_extract(m.DATA, '\$.reactions')) AS reactions,
          MAX(json_extract(m.DATA, '\$.reply_count')) AS reply_count,
          MAX(json_extract(m.DATA, '\$.attachments')) AS attachments
        FROM ${msg_table} m
        LEFT JOIN ${ch_dedup} c ON c.ID = m.CHANNEL_ID
        LEFT JOIN ${user_dedup} u ON u.ID = ${user_expr}
        WHERE (LOWER(COALESCE(c.NAME,'')) = LOWER('${channel_filter}') OR m.CHANNEL_ID = '${channel_filter}')
          AND (m.${text_col} IS NOT NULL AND m.${text_col} != '' OR m.NUM_FILES > 0)
        GROUP BY m.TS, m.CHANNEL_ID
        ORDER BY CAST(m.TS AS REAL) ASC;
      " 2>/dev/null || echo "[]")

      [[ "$rows_json" == "[]" || -z "$rows_json" ]] && continue

      python3 - "$db" "$user_table" "$format" "$channel_filter" "$rows_json" "$deeplinks" "$ws" "$ws_dir" <<'PYEOF'
import html as html_mod
import json, os, re, sqlite3, sys, urllib.request, urllib.error
from datetime import datetime, timezone

db_path, user_table, fmt, ch_filter = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
rows = json.loads(sys.argv[5])
deeplinks_on = sys.argv[6] == "1" if len(sys.argv) > 6 else True
workspace = sys.argv[7] if len(sys.argv) > 7 else ""
ws_dir = sys.argv[8] if len(sys.argv) > 8 else ""
uploads_dir = os.path.join(ws_dir, "__uploads") if ws_dir else ""
if not rows:
    sys.exit(0)

# Resolve channel display name from first row
ch_name = rows[0].get("channel", "") or ch_filter
channel_id = rows[0].get("channel_id", "")

conn = sqlite3.connect(db_path)
user_map = {}
avatar_map = {}
try:
    for r in conn.execute(f"SELECT ID, USERNAME FROM {user_table} GROUP BY ID"):
        user_map[r[0]] = r[1]
except Exception:
    try:
        for r in conn.execute(f"SELECT ID, COALESCE(real_name, name) FROM {user_table} GROUP BY ID"):
            user_map[r[0]] = r[1]
    except Exception:
        pass
# Try to load profile pics from DATA JSON column
try:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({user_table})").fetchall()]
    if "DATA" in cols:
        for r in conn.execute(f"SELECT ID, json_extract(DATA, '$.profile.image_48') FROM {user_table} WHERE json_extract(DATA, '$.profile.image_48') IS NOT NULL GROUP BY ID"):
            if r[1]:
                avatar_map[r[0]] = r[1]
    elif "profile" in cols:
        for r in conn.execute(f"SELECT ID, json_extract(profile, '$.image_48') FROM {user_table} WHERE json_extract(profile, '$.image_48') IS NOT NULL GROUP BY ID"):
            if r[1]:
                avatar_map[r[0]] = r[1]
except Exception:
    pass
# Try to get team_id from first user's DATA
team_id = ""
try:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({user_table})").fetchall()]
    if "DATA" in cols:
        row = conn.execute(f"SELECT json_extract(DATA, '$.team_id') FROM {user_table} WHERE json_extract(DATA, '$.team_id') IS NOT NULL LIMIT 1").fetchone()
        if row and row[0]:
            team_id = row[0]
except Exception:
    pass
conn.close()

# Color palette for initial-based fallback avatars
_AVATAR_COLORS = ["#e8912d", "#2bac76", "#e01e5a", "#1264a3", "#8c5db0",
                   "#d4a843", "#e25950", "#36c5f0", "#4a154b", "#2eb67d"]
def avatar_fallback(name, uid=""):
    """Generate an SVG data URI with the user's initial."""
    initial = (name or "?")[0].upper()
    idx = sum(ord(c) for c in (uid or name or "?")) % len(_AVATAR_COLORS)
    color = _AVATAR_COLORS[idx]
    svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="36" height="36"><rect width="36" height="36" rx="4" fill="{color}"/><text x="18" y="24" text-anchor="middle" fill="white" font-family="system-ui,sans-serif" font-size="18" font-weight="600">{initial}</text></svg>'
    import base64
    return "data:image/svg+xml;base64," + base64.b64encode(svg.encode()).decode()

# ── Emoji map (top ~100 Slack emoji → Unicode) ──
EMOJI_MAP = {
    "smile": "\U0001f604", "laughing": "\U0001f606", "blush": "\U0001f60a",
    "smiley": "\U0001f603", "relaxed": "\u263a\ufe0f", "smirk": "\U0001f60f",
    "heart_eyes": "\U0001f60d", "kissing_heart": "\U0001f618",
    "kissing_closed_eyes": "\U0001f61a", "flushed": "\U0001f633",
    "relieved": "\U0001f60c", "satisfied": "\U0001f606", "grin": "\U0001f601",
    "wink": "\U0001f609", "stuck_out_tongue_winking_eye": "\U0001f61c",
    "stuck_out_tongue_closed_eyes": "\U0001f61d", "grinning": "\U0001f600",
    "kissing": "\U0001f617", "stuck_out_tongue": "\U0001f61b",
    "sleeping": "\U0001f634", "worried": "\U0001f61f", "frowning": "\U0001f626",
    "anguished": "\U0001f627", "open_mouth": "\U0001f62e", "grimacing": "\U0001f62c",
    "confused": "\U0001f615", "hushed": "\U0001f62f", "expressionless": "\U0001f611",
    "unamused": "\U0001f612", "sweat_smile": "\U0001f605", "sweat": "\U0001f613",
    "disappointed_relieved": "\U0001f625", "weary": "\U0001f629",
    "pensive": "\U0001f614", "disappointed": "\U0001f61e", "confounded": "\U0001f616",
    "fearful": "\U0001f628", "cold_sweat": "\U0001f630", "persevere": "\U0001f623",
    "cry": "\U0001f622", "sob": "\U0001f62d", "joy": "\U0001f602",
    "astonished": "\U0001f632", "scream": "\U0001f631", "tired_face": "\U0001f62b",
    "angry": "\U0001f620", "rage": "\U0001f621", "triumph": "\U0001f624",
    "sleepy": "\U0001f62a", "yum": "\U0001f60b", "mask": "\U0001f637",
    "sunglasses": "\U0001f60e", "dizzy_face": "\U0001f635",
    "imp": "\U0001f47f", "smiling_imp": "\U0001f608", "neutral_face": "\U0001f610",
    "no_mouth": "\U0001f636", "innocent": "\U0001f607", "alien": "\U0001f47d",
    "yellow_heart": "\U0001f49b", "blue_heart": "\U0001f499",
    "purple_heart": "\U0001f49c", "heart": "\u2764\ufe0f", "green_heart": "\U0001f49a",
    "broken_heart": "\U0001f494", "heartbeat": "\U0001f493",
    "heartpulse": "\U0001f497", "sparkling_heart": "\U0001f496",
    "star": "\u2b50", "star2": "\U0001f31f", "dizzy": "\U0001f4ab",
    "boom": "\U0001f4a5", "fire": "\U0001f525", "100": "\U0001f4af",
    "thumbsup": "\U0001f44d", "+1": "\U0001f44d", "thumbsdown": "\U0001f44e",
    "-1": "\U0001f44e", "ok_hand": "\U0001f44c", "punch": "\U0001f44a",
    "fist": "\u270a", "v": "\u270c\ufe0f", "wave": "\U0001f44b",
    "hand": "\u270b", "open_hands": "\U0001f450", "point_up": "\u261d\ufe0f",
    "point_down": "\U0001f447", "point_left": "\U0001f448",
    "point_right": "\U0001f449", "raised_hands": "\U0001f64c",
    "pray": "\U0001f64f", "clap": "\U0001f44f", "muscle": "\U0001f4aa",
    "eyes": "\U0001f440", "tongue": "\U0001f445", "lips": "\U0001f444",
    "tada": "\U0001f389", "sparkles": "\u2728", "balloon": "\U0001f388",
    "party_popper": "\U0001f389", "gift": "\U0001f381",
    "bell": "\U0001f514", "rocket": "\U0001f680",
    "thinking_face": "\U0001f914", "thinking": "\U0001f914",
    "face_with_rolling_eyes": "\U0001f644", "rolling_eyes": "\U0001f644",
    "slightly_smiling_face": "\U0001f642", "slightly_frowning_face": "\U0001f641",
    "upside_down_face": "\U0001f643", "zipper_mouth_face": "\U0001f910",
    "money_mouth_face": "\U0001f911", "nerd_face": "\U0001f913",
    "hugging_face": "\U0001f917", "hugging": "\U0001f917",
    "skull": "\U0001f480", "robot_face": "\U0001f916",
    "see_no_evil": "\U0001f648", "hear_no_evil": "\U0001f649",
    "speak_no_evil": "\U0001f64a", "white_check_mark": "\u2705",
    "heavy_check_mark": "\u2714\ufe0f", "x": "\u274c",
    "warning": "\u26a0\ufe0f", "exclamation": "\u2757",
    "question": "\u2753", "bulb": "\U0001f4a1", "mega": "\U0001f4e3",
    "rotating_light": "\U0001f6a8", "lock": "\U0001f512", "key": "\U0001f511",
    "mag": "\U0001f50d", "link": "\U0001f517", "email": "\U0001f4e7",
    "phone": "\u260e\ufe0f", "computer": "\U0001f4bb", "speech_balloon": "\U0001f4ac",
    "thought_balloon": "\U0001f4ad", "calendar": "\U0001f4c5",
    "chart_with_upwards_trend": "\U0001f4c8", "heavy_plus_sign": "\u2795",
    "heavy_minus_sign": "\u2796", "arrow_right": "\u27a1\ufe0f",
    "large_blue_diamond": "\U0001f537", "large_orange_diamond": "\U0001f536",
    "small_blue_diamond": "\U0001f539", "small_orange_diamond": "\U0001f538",
}
SKIN_TONES = {
    "skin-tone-2": "\U0001f3fb", "skin-tone-3": "\U0001f3fc",
    "skin-tone-4": "\U0001f3fd", "skin-tone-5": "\U0001f3fe",
    "skin-tone-6": "\U0001f3ff",
}

def convert_emoji(text):
    """Convert :emoji: and :emoji::skin-tone-N: to Unicode."""
    def _repl(m):
        name = m.group(1)
        # Check for skin tone modifier
        skin = ""
        parts = name.split("::")
        if len(parts) == 2:
            name = parts[0]
            skin = SKIN_TONES.get(parts[1], "")
        elif ":" in name:
            # :thumbsup::skin-tone-3: pattern captured differently
            pass
        base = EMOJI_MAP.get(name)
        if base:
            return base + skin
        return m.group(0)  # leave unknown emoji as-is
    # Handle :emoji::skin-tone-N: (two colons between)
    text = re.sub(r':([a-z0-9_+-]+(?:::skin-tone-[2-6])?):',  _repl, text)
    return text

# ── Syntax highlighting (simple regex-based) ──
SYNTAX_RULES = {
    "python": [
        (r'\b(def|class|import|from|return|if|elif|else|for|while|with|as|try|except|finally|raise|yield|lambda|and|or|not|in|is|True|False|None|pass|break|continue|assert|global|nonlocal|del|async|await)\b', 'kw'),
        (r'("""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'|"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\')', 'str'),
        (r'(#[^\n]*)', 'cmt'),
        (r'\b(\d+\.?\d*)\b', 'num'),
    ],
    "javascript": [
        (r'\b(const|let|var|function|return|if|else|for|while|do|switch|case|break|continue|new|this|class|extends|import|export|default|from|try|catch|finally|throw|async|await|yield|typeof|instanceof|of|in|true|false|null|undefined|void)\b', 'kw'),
        (r'(`(?:[^`\\]|\\.)*`|"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\')', 'str'),
        (r'(\/\/[^\n]*|\/\*[\s\S]*?\*\/)', 'cmt'),
        (r'\b(\d+\.?\d*)\b', 'num'),
    ],
    "bash": [
        (r'\b(if|then|else|elif|fi|for|do|done|while|until|case|esac|function|return|local|export|source|alias|unset|readonly|shift|exit|break|continue|declare|typeset|trap|eval|exec)\b', 'kw'),
        (r'("(?:[^"\\]|\\.)*"|\'[^\']*\')', 'str'),
        (r'(#[^\n]*)', 'cmt'),
        (r'\b(\d+)\b', 'num'),
    ],
    "sql": [
        (r'(?i)\b(SELECT|FROM|WHERE|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TABLE|INDEX|INTO|VALUES|SET|JOIN|LEFT|RIGHT|INNER|OUTER|ON|AND|OR|NOT|NULL|IS|IN|BETWEEN|LIKE|ORDER|BY|GROUP|HAVING|LIMIT|OFFSET|AS|UNION|ALL|DISTINCT|EXISTS|CASE|WHEN|THEN|ELSE|END|COUNT|SUM|AVG|MAX|MIN|COALESCE|CAST|PRIMARY|KEY|FOREIGN|REFERENCES|CONSTRAINT|DEFAULT|CHECK|UNIQUE|VIEW|TRIGGER|PROCEDURE|FUNCTION|BEGIN|COMMIT|ROLLBACK)\b', 'kw'),
        (r"('(?:[^'\\]|\\.)*')", 'str'),
        (r'(--[^\n]*)', 'cmt'),
        (r'\b(\d+\.?\d*)\b', 'num'),
    ],
    "json": [
        (r'("(?:[^"\\]|\\.)*")\s*:', 'kw'),
        (r':\s*("(?:[^"\\]|\\.)*")', 'str'),
        (r'\b(true|false|null)\b', 'kw'),
        (r'\b(\d+\.?\d*)\b', 'num'),
    ],
}
# Aliases
SYNTAX_RULES["py"] = SYNTAX_RULES["python"]
SYNTAX_RULES["js"] = SYNTAX_RULES["javascript"]
SYNTAX_RULES["ts"] = SYNTAX_RULES["javascript"]
SYNTAX_RULES["typescript"] = SYNTAX_RULES["javascript"]
SYNTAX_RULES["sh"] = SYNTAX_RULES["bash"]
SYNTAX_RULES["shell"] = SYNTAX_RULES["bash"]
SYNTAX_RULES["zsh"] = SYNTAX_RULES["bash"]

def highlight_code(code, lang):
    """Apply simple regex-based syntax highlighting."""
    lang = (lang or "").strip().lower()
    rules = SYNTAX_RULES.get(lang)
    if not rules:
        return html_mod.escape(code)
    escaped = html_mod.escape(code)
    # Apply rules with placeholder approach to avoid double-matching
    placeholders = []
    def _placeholder(cls, text):
        idx = len(placeholders)
        placeholders.append(f'<span class="hl-{cls}">{text}</span>')
        return f"\x00PH{idx}\x00"
    for pattern, cls in rules:
        def _repl(m, cls=cls):
            return _placeholder(cls, m.group(0))
        escaped = re.sub(pattern, _repl, escaped)
    # Restore placeholders
    for i, val in enumerate(placeholders):
        escaped = escaped.replace(f"\x00PH{i}\x00", val)
    return escaped

def render_code_blocks(text_html):
    """Convert fenced code blocks and inline code in already-escaped HTML."""
    # Fenced code blocks (```lang\n...\n```)
    def _fenced(m):
        lang = m.group(1) or ""
        code = m.group(2)
        # Unescape <br> back to newlines for code
        code = code.replace("<br>", "\n").replace("&lt;br&gt;", "\n")
        label = f'<span class="code-lang">{html_mod.escape(lang)}</span>' if lang else ""
        # Re-highlight (code was already escaped; unescape for re-processing)
        raw = html_mod.unescape(code)
        highlighted = highlight_code(raw, lang)
        return f'{label}<pre class="code-block"><code>{highlighted}</code></pre>'
    text_html = re.sub(r'```(\w*)\s*<br>([\s\S]*?)```', _fenced, text_html)
    # Inline code
    text_html = re.sub(r'`([^`\n]+?)`', r'<code class="inline-code">\1</code>', text_html)
    return text_html

# ── Link preview (OpenGraph) ──
_og_cache = {}
def fetch_og_preview(url):
    """Fetch OpenGraph metadata for a URL. Returns dict or None."""
    if url in _og_cache:
        return _og_cache[url]
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "SlackslawExport/1.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            content = resp.read(64 * 1024).decode("utf-8", errors="replace")
        og = {}
        for prop in ("og:title", "og:description", "og:image"):
            m = re.search(rf'<meta\s+(?:property|name)="{re.escape(prop)}"\s+content="([^"]*)"', content, re.I)
            if not m:
                m = re.search(rf'<meta\s+content="([^"]*)"\s+(?:property|name)="{re.escape(prop)}"', content, re.I)
            if m:
                og[prop] = html_mod.unescape(m.group(1))
        _og_cache[url] = og if og else None
        return _og_cache[url]
    except Exception:
        _og_cache[url] = None
        return None

def render_link_preview(url):
    """Return HTML for a link preview card, or empty string."""
    og = fetch_og_preview(url)
    if not og:
        return ""
    title = html_mod.escape(og.get("og:title", ""))
    desc = html_mod.escape(og.get("og:description", ""))
    img = og.get("og:image", "")
    parts = ['<div class="link-preview">']
    if title:
        parts.append(f'<div class="lp-title">{title}</div>')
    if desc:
        parts.append(f'<div class="lp-desc">{desc}</div>')
    if img:
        parts.append(f'<img class="lp-img" src="{html_mod.escape(img)}" loading="lazy" alt="">')
    parts.append('</div>')
    return "".join(parts) if title or desc else ""

# ── Slack mrkdwn → HTML (token-placeholder pattern) ──
def prettify_text_html(text):
    """Convert Slack mrkdwn to HTML using token-placeholder pattern."""
    if not text:
        return ""
    placeholders = []
    def _ph(html_fragment):
        idx = len(placeholders)
        placeholders.append(html_fragment)
        return f"\x00TK{idx}\x00"

    # Step 1: Extract Slack tokens and replace with placeholders
    def _repl_mention(m):
        uid = m.group(1)
        name = user_map.get(uid, uid)
        if deeplinks_on and team_id:
            return _ph(f'<a class="mention" href="https://app.slack.com/client/{team_id}/{uid}">@{html_mod.escape(name)}</a>')
        return _ph(f'<span class="mention">@{html_mod.escape(name)}</span>')

    def _repl_channel(m):
        cid = m.group(1)
        cname = m.group(2)
        if deeplinks_on and team_id:
            return _ph(f'<a class="mention" href="https://app.slack.com/client/{team_id}/{cid}">#{html_mod.escape(cname)}</a>')
        return _ph(f'<span class="mention">#{html_mod.escape(cname)}</span>')

    def _repl_url_labeled(m):
        url, label = m.group(1), m.group(2)
        preview = render_link_preview(url)
        return _ph(f'<a href="{html_mod.escape(url)}" target="_blank" class="link">{html_mod.escape(label)}</a>{preview}')

    def _repl_url_bare(m):
        url = m.group(1)
        preview = render_link_preview(url)
        return _ph(f'<a href="{html_mod.escape(url)}" target="_blank" class="link">{html_mod.escape(url)}</a>{preview}')

    def _repl_mailto(m):
        addr, label = m.group(1), m.group(2)
        return _ph(f'<a href="mailto:{html_mod.escape(addr)}" class="link">{html_mod.escape(label)}</a>')

    def _repl_special(m):
        cmd = m.group(1)
        return _ph(f'<span class="mention">@{html_mod.escape(cmd)}</span>')

    text = re.sub(r'<@([A-Z0-9]+)(?:\|[^>]*)?>', _repl_mention, text)
    text = re.sub(r'<#([A-Z0-9]+)\|([^>]+)>', _repl_channel, text)
    text = re.sub(r'<mailto:([^|>]+)\|([^>]+)>', _repl_mailto, text)
    text = re.sub(r'<(https?://[^|>]+)\|([^>]+)>', _repl_url_labeled, text)
    text = re.sub(r'<(https?://[^>]+)>', _repl_url_bare, text)
    text = re.sub(r'<!([a-z]+)(?:\|[^>]*)?>', _repl_special, text)

    # Step 2: HTML-escape the remaining text
    text = html_mod.escape(text)
    text = text.replace("\n", "<br>")

    # Step 3: Restore placeholders
    for i, val in enumerate(placeholders):
        text = text.replace(f"\x00TK{i}\x00", val)

    # Step 4: Emoji conversion
    text = convert_emoji(text)

    # Step 5: Code blocks (must come before bold/italic to avoid conflicts)
    text = render_code_blocks(text)

    # Step 6: Bold, italic, strikethrough
    text = re.sub(r'(?<![\\])\*([^\*\n]+?)\*', r'<strong>\1</strong>', text)
    text = re.sub(r'(?<![\\])_([^_\n]+?)_', r'<em>\1</em>', text)
    text = re.sub(r'(?<![\\])~([^~\n]+?)~', r'<del>\1</del>', text)

    return text

def prettify_text(text):
    """Markdown-flavored prettify (unchanged from original)."""
    if not text:
        return ""
    def repl_mention(m):
        return "@" + user_map.get(m.group(1), m.group(1))
    text = re.sub(r'<@([A-Z0-9]+)(?:\|[^>]*)?>', repl_mention, text)
    text = re.sub(r'<mailto:([^|>]+)\|([^>]+)>', r'\2', text)
    def repl_url(m):
        return f"[{m.group(2)}]({m.group(1)})"
    text = re.sub(r'<(https?://[^|>]+)\|([^>]+)>', repl_url, text)
    text = re.sub(r'<(https?://[^>]+)>', r'\1', text)
    text = re.sub(r'<#[A-Z0-9]+\|([^>]+)>', r'#\1', text)
    text = re.sub(r'<!([a-z]+)(?:\|[^>]*)?>', r'@\1', text)
    return text

# ── Read local file and encode as data URI ──
import base64 as _b64
def local_file_data_uri(file_id, filename, mime):
    """Read file from local __uploads dir and return as data:... URI."""
    if not uploads_dir or not file_id:
        return None
    path = os.path.join(uploads_dir, file_id, filename)
    if not os.path.isfile(path):
        # Try finding any file in the file_id directory
        alt_dir = os.path.join(uploads_dir, file_id)
        if os.path.isdir(alt_dir):
            entries = os.listdir(alt_dir)
            if entries:
                path = os.path.join(alt_dir, entries[0])
            else:
                return None
        else:
            return None
    try:
        with open(path, "rb") as f:
            data = f.read(10 * 1024 * 1024)  # 10MB max
        return f"data:{mime};base64,{_b64.b64encode(data).decode()}"
    except Exception:
        return None

# ── Render files/images ──
def render_files(files_json):
    if not files_json:
        return ""
    try:
        files = json.loads(files_json) if isinstance(files_json, str) else files_json
    except (json.JSONDecodeError, TypeError):
        return ""
    if not files:
        return ""
    parts = []
    for f in files:
        mime = f.get("mimetype", "") or ""
        name = f.get("name", "file")
        file_id = f.get("id", "")
        url = f.get("url_private", "") or f.get("permalink", "") or ""
        if mime.startswith("image/"):
            data_uri = local_file_data_uri(file_id, name, mime)
            if data_uri:
                parts.append(f'<div class="file-img"><img src="{data_uri}" alt="{html_mod.escape(name)}"></div>')
            elif url:
                parts.append(f'<div class="file-img"><img src="{html_mod.escape(url)}" alt="{html_mod.escape(name)}" loading="lazy"></div>')
        elif mime.startswith(("video/", "audio/")):
            tag = "video" if mime.startswith("video/") else "audio"
            data_uri = local_file_data_uri(file_id, name, mime)
            if data_uri:
                parts.append(f'<div class="file-media"><{tag} controls src="{data_uri}"></{tag}></div>')
            elif url:
                parts.append(f'<div class="file-attach">\U0001f4ce <a href="{html_mod.escape(url)}" target="_blank">{html_mod.escape(name)}</a></div>')
        else:
            data_uri = local_file_data_uri(file_id, name, mime)
            if data_uri:
                parts.append(f'<div class="file-attach">\U0001f4ce <a href="{data_uri}" download="{html_mod.escape(name)}">{html_mod.escape(name)}</a></div>')
            elif url:
                parts.append(f'<div class="file-attach">\U0001f4ce <a href="{html_mod.escape(url)}" target="_blank">{html_mod.escape(name)}</a></div>')
            else:
                parts.append(f'<div class="file-attach">\U0001f4ce {html_mod.escape(name)}</div>')
    return "".join(parts)

# ── Render reactions ──
def render_reactions(reactions_json):
    if not reactions_json:
        return ""
    try:
        reactions = json.loads(reactions_json) if isinstance(reactions_json, str) else reactions_json
    except (json.JSONDecodeError, TypeError):
        return ""
    if not reactions:
        return ""
    parts = ['<div class="reactions">']
    for rxn in reactions:
        name = rxn.get("name", "")
        count = rxn.get("count", 1)
        emoji = EMOJI_MAP.get(name, f":{name}:")
        parts.append(f'<span class="reaction">{emoji} {count}</span>')
    parts.append('</div>')
    return "".join(parts)

# ── Deep link for message timestamp ──
def msg_deeplink(ts, ch_id):
    if not deeplinks_on or not team_id or not ts or not ch_id:
        return ""
    ts_nodot = ts.replace(".", "")
    return f"https://app.slack.com/archives/{ch_id}/p{ts_nodot}"

# ── Single message renderer ──
def render_message(r, is_reply=False):
    ts = r.get("ts", "")
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except Exception:
        dt = ts
    author = r.get("author", "unknown")
    uid = r.get("user_id", "")
    text = r.get("text", "") or r.get("TXT", "") or ""
    text_html = prettify_text_html(text)
    files_html = render_files(r.get("files"))
    reactions_html = render_reactions(r.get("reactions"))
    reply_count = r.get("reply_count")

    css_class = "reply" if is_reply else "msg"

    # Avatar
    avatar_url = avatar_map.get(uid, "") if uid else ""
    if not avatar_url:
        avatar_url = avatar_fallback(author, uid)
    avatar_size = "28" if is_reply else "36"
    avatar_html = f'<img class="avatar" src="{html_mod.escape(avatar_url)}" width="{avatar_size}" height="{avatar_size}" alt="">'

    # Author with optional deep link
    if deeplinks_on and team_id and uid:
        author_html = f'<a class="author" href="https://app.slack.com/client/{team_id}/{uid}">{html_mod.escape(author)}</a>'
    else:
        author_html = f'<span class="author">{html_mod.escape(author)}</span>'

    # Timestamp with optional deep link (light gray, next to author)
    link = msg_deeplink(ts, r.get("channel_id", channel_id))
    if link:
        time_html = f'<a class="time" href="{link}" target="_blank">{dt}</a>'
    else:
        time_html = f'<span class="time">{dt}</span>'

    # Reply count badge
    badge = ""
    if reply_count and not is_reply:
        badge = f' <span class="reply-badge">{reply_count} {"reply" if reply_count == 1 else "replies"}</span>'

    out = (f'<div class="{css_class}">'
           f'<div class="msg-row">{avatar_html}'
           f'<div class="msg-content">'
           f'<div class="msg-header">{author_html}{time_html}{badge}</div>'
           f'<div class="text">{text_html}</div>{files_html}{reactions_html}'
           f'</div></div>')
    return out

# ── Main output ──
if fmt == "html":
    CSS = """
body { font-family: 'Slack-Lato', system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
  max-width: 820px; margin: 2em auto; padding: 0 1.2em; line-height: 1.6; color: #1d1c1d; background: #fff; }
h1 { font-size: 1.4em; border-bottom: 1px solid #e8e8e8; padding-bottom: 0.5em; }
.msg { border-bottom: 1px solid #e8e8e8; padding: 10px 0; }
.reply { padding: 6px 0 6px 28px; border-left: 3px solid #e0e0e0; margin-left: 16px; margin-top: 4px; }
.thread { margin-top: 4px; }
.msg-row { display: flex; align-items: flex-start; gap: 10px; }
.msg-content { flex: 1; min-width: 0; }
.msg-header { display: flex; align-items: baseline; flex-wrap: wrap; gap: 0; }
.avatar { border-radius: 4px; flex-shrink: 0; margin-top: 2px; }
.author, a.author { font-weight: 700; color: #1d1c1d; text-decoration: none; }
a.author:hover { text-decoration: underline; color: #1264a3; }
.time, a.time { color: #ababad; font-size: 0.82em; margin-left: 8px; text-decoration: none; font-weight: 400; }
a.time:hover { text-decoration: underline; color: #1264a3; }
.reply-badge { font-size: 0.78em; color: #1264a3; background: #e8f5fe; padding: 1px 7px;
  border-radius: 10px; margin-left: 8px; }
.text { margin-top: 4px; white-space: pre-wrap; word-wrap: break-word; }
.mention, a.mention { color: #1264a3; background: #e8f5fe; padding: 1px 4px; border-radius: 3px;
  text-decoration: none; font-weight: 500; }
a.mention:hover { text-decoration: underline; }
.link { color: #1264a3; text-decoration: underline; }
.link:hover { color: #0b4c8c; }
.code-block { background: #f8f8f8; border: 1px solid #e1e1e1; border-radius: 6px; padding: 12px;
  overflow-x: auto; font-family: 'Monaco', 'Menlo', 'Consolas', monospace; font-size: 0.88em;
  line-height: 1.45; margin: 6px 0; white-space: pre; }
.code-lang { font-size: 0.75em; color: #888; background: #f0f0f0; padding: 1px 6px;
  border-radius: 3px; display: inline-block; margin-bottom: 4px; }
.inline-code { background: #f0f0f0; border: 1px solid #e1e1e1; border-radius: 3px; padding: 1px 4px;
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace; font-size: 0.88em; color: #e01e5a; }
.hl-kw { color: #d73a49; font-weight: 600; }
.hl-str { color: #032f62; }
.hl-cmt { color: #6a737d; font-style: italic; }
.hl-num { color: #005cc5; }
.file-img { margin: 8px 0; }
.file-img img { max-width: 480px; max-height: 360px; border-radius: 6px; border: 1px solid #e1e1e1; }
.file-attach { margin: 4px 0; font-size: 0.92em; }
.file-attach a { color: #1264a3; text-decoration: underline; }
.file-media { margin: 8px 0; }
.file-media video, .file-media audio { max-width: 480px; border-radius: 6px; }
.reactions { margin-top: 6px; display: flex; flex-wrap: wrap; gap: 4px; }
.reaction { background: #f0f0f0; border: 1px solid #e1e1e1; border-radius: 12px; padding: 2px 8px;
  font-size: 0.85em; display: inline-flex; align-items: center; gap: 4px; }
.link-preview { border-left: 4px solid #e0e0e0; padding: 8px 12px; margin: 6px 0;
  background: #fafafa; border-radius: 0 6px 6px 0; max-width: 500px; }
.lp-title { font-weight: 600; font-size: 0.92em; color: #1264a3; }
.lp-desc { font-size: 0.85em; color: #616061; margin-top: 2px; }
.lp-img { max-width: 200px; max-height: 120px; border-radius: 4px; margin-top: 4px; }
.export-footer { margin-top: 2em; padding: 1em 0; border-top: 1px solid #e8e8e8;
  color: #ababad; font-size: 0.82em; text-align: center; }
"""
    print("<!DOCTYPE html><html><head><meta charset='utf-8'>")
    print(f"<title>#{html_mod.escape(ch_name)}</title>")
    print(f"<style>{CSS}</style></head>")
    print(f'<body><h1>#{html_mod.escape(ch_name)}</h1>')

    # Group into threads: parents and replies
    parents = []
    threads = {}  # thread_ts -> [replies]
    for r in rows:
        ts = r.get("ts", "")
        tts = r.get("thread_ts")
        if not tts or tts == ts:
            parents.append(r)
        else:
            threads.setdefault(tts, []).append(r)

    for r in parents:
        ts = r.get("ts", "")
        out = render_message(r, is_reply=False)
        replies = threads.get(ts, [])
        if replies:
            out += '<div class="thread">'
            for reply in replies:
                out += render_message(reply, is_reply=True) + '</div>'
            out += '</div>'
        out += '</div>'
        print(out)

    export_time = datetime.now().strftime("%B %d, %Y at %H:%M")
    print(f'<footer class="export-footer">Exported with slackslaw on {html_mod.escape(export_time)}</footer>')
    print("</body></html>")
else:
    # Markdown (unchanged)
    print(f"## #{ch_name}\n")
    for r in rows:
        ts = r.get("ts", "")
        try:
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        except Exception:
            dt = ts
        author = r.get("author", "unknown")
        text = prettify_text(r.get("text", "") or r.get("TXT", "") or "")
        print(f"**{author}** ({dt}):\n{text}\n\n---\n")
    export_time = datetime.now().strftime("%B %d, %Y at %H:%M")
    print(f"\n---\n\n*Exported with slackslaw on {export_time}*\n")
PYEOF
    done <<< "$workspaces"
  } | output_func

  [[ -n "$output_file" ]] && info "Exported to ${output_file}"
}

cmd_gc() {
  ensure_sqlite3
  init_dirs

  local workspaces
  workspaces=$(list_configured_workspaces)
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  header "Garbage Collection"

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")
    [[ -z "$db" || ! -f "$db" ]] && continue

    local size_before size_after
    size_before=$(du -sh "$db" 2>/dev/null | cut -f1)

    info "Vacuuming ${ws} (${size_before})…"
    sqlite3 "$db" "VACUUM;" 2>/dev/null || { warn "VACUUM failed for ${ws}"; continue; }

    size_after=$(du -sh "$db" 2>/dev/null | cut -f1)
    echo -e "  ${ws}: ${size_before} → ${size_after}"

    # Clean up orphaned WAL/SHM files
    for ext in -wal -shm; do
      if [[ -f "${db}${ext}" ]]; then
        rm -f "${db}${ext}"
        echo -e "  ${DIM}Removed orphaned ${db}${ext}${RESET}"
      fi
    done
  done <<< "$workspaces"

  info "Done."
}

cmd_repair() {
  ensure_sqlite3
  init_dirs

  local workspaces
  workspaces=$(list_configured_workspaces)
  [[ -z "$workspaces" ]] && die "No workspaces configured."

  header "Database Integrity Check"

  while IFS= read -r ws; do
    [[ -z "$ws" ]] && continue
    local ws_dir db
    ws_dir=$(workspace_dir "$ws")
    db=$(find_workspace_db "$ws_dir")
    [[ -z "$db" || ! -f "$db" ]] && continue

    local result
    result=$(sqlite3 "$db" "PRAGMA integrity_check;" 2>/dev/null || echo "error")

    if [[ "$result" == "ok" ]]; then
      echo -e "  ${GREEN}✓${RESET} ${ws}: database OK"
    else
      echo -e "  ${RED}✗${RESET} ${ws}: integrity check failed"
      echo -e "    ${DIM}${result}${RESET}"
      warn "Re-run a full sync to rebuild: slackslaw sync --full --workspace ${ws}"
    fi
  done <<< "$workspaces"
}

cmd_workspaces_remove() {
  init_dirs

  local ws_name="${1:-}"
  local delete_data=false

  shift 2>/dev/null || true
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --delete-data) delete_data=true ;;
      *)             warn "Unknown option: $1" ;;
    esac
    shift
  done

  [[ -z "$ws_name" ]] && die "Usage: slackslaw workspaces remove <name> [--delete-data]"

  # Normalise
  [[ "$ws_name" != *.slack.com ]] && ws_name="${ws_name}.slack.com"

  # Check if it exists
  local existing
  existing=$(json_array_get "$CONFIG_FILE" "workspaces" | grep -Fx "$ws_name" || true)
  [[ -z "$existing" ]] && die "Workspace '${ws_name}' not found in config."

  json_remove_from_array "$CONFIG_FILE" "workspaces" "$ws_name"
  info "Removed workspace '${ws_name}' from config."

  if $delete_data; then
    local ws_dir
    ws_dir=$(workspace_dir "$ws_name")
    if [[ -d "$ws_dir" ]]; then
      rm -rf "$ws_dir"
      info "Deleted archive directory: ${ws_dir}"
    fi
  fi
}

cmd_help() {
  cat <<EOF

${BOLD}slackslaw${RESET} — slackslaw, the CLI for Slack

${BOLD}USAGE${RESET}
  slackslaw <command> [options]

${BOLD}COMMANDS${RESET}
  ${CYAN}auth${RESET}                         Add / re-authenticate a Slack workspace
  ${CYAN}workspaces${RESET}  (alias: ws)      List configured workspaces and stats
  ${CYAN}workspaces${RESET} add <name>        Alias for auth
  ${CYAN}workspaces${RESET} remove <name>     Remove a workspace [--delete-data]
  ${CYAN}keywords${RESET}   (alias: kw)      List configured channel keywords
  ${CYAN}keywords${RESET} add <word>          Add a keyword (used by --mine filter)
  ${CYAN}keywords${RESET} remove <word>       Remove a keyword
  ${CYAN}sync${RESET} [--full]                Sync workspaces (incremental by default)
  ${CYAN}sync${RESET} --workspace <n>         Sync a single workspace
  ${CYAN}sync${RESET} --channel "#name"       Sync a single channel
  ${CYAN}sync${RESET} --since 30d             Only sync messages from the last 30d/24h/etc
  ${CYAN}sync${RESET} --quiet                 Cron-friendly: suppress output, exit 0/1
  ${CYAN}whatsnew${RESET}    (alias: new)     Messages received since the last sync
  ${CYAN}whatsnew${RESET} --mine              Only messages relevant to me (+ keywords)
  ${CYAN}whatsnew${RESET} --json              Same, as JSONL (one object per line)
  ${CYAN}unread${RESET}                       Show messages marked as unread
  ${CYAN}unread${RESET} --json                Same, as JSONL (one object per line)
  ${CYAN}search${RESET} "expr"                Full-text search across all workspaces
  ${CYAN}search${RESET} "expr" --workspace W  Limit to one workspace
  ${CYAN}search${RESET} "expr" --channel C    Limit to channel (# prefix optional)
  ${CYAN}search${RESET} "expr" --author A     Limit to author name
  ${CYAN}search${RESET} "expr" --mine         Only threads I'm in
  ${CYAN}search${RESET} "expr" --after 7d     Only messages after date/duration
  ${CYAN}search${RESET} "expr" --before DATE  Only messages before date/duration
  ${CYAN}search${RESET} "expr" --slackmd      Raw Slack mrkdwn output (no prettify)
  ${CYAN}search${RESET} "expr" --format json  Same as --json (works on all commands)
  ${CYAN}search${RESET} "expr" --newest-first Show newest results first (default: oldest)
  ${CYAN}search${RESET} "expr" --limit N      Max results (default: 20)
  ${CYAN}search${RESET} "expr" --json         JSONL output
  ${CYAN}threads${RESET} <url>                Dump a thread by Slack URL [--json]
  ${CYAN}stats${RESET}                        Detailed archive statistics
  ${CYAN}export${RESET} --channel "#name"     Export channel to markdown or HTML
  ${CYAN}export${RESET} --format html         Export as HTML (default: md)
  ${CYAN}export${RESET} --output file.md      Write to file instead of stdout
  ${CYAN}tail${RESET} -f                      Watch for new messages (live poll)
  ${CYAN}tail${RESET} --interval 10           Set poll interval in seconds (default: 30)
  ${CYAN}context${RESET} --since 7d           LLM-ready JSON context dump
  ${CYAN}ask${RESET} "question"               RAG: ask a question over the archive
  ${CYAN}ask${RESET} --provider openai        Use OpenAI instead of Anthropic
  ${CYAN}mcp${RESET}                          Start MCP server (wraps slackdump mcp)
  ${CYAN}gc${RESET}                           Garbage collect (VACUUM databases)
  ${CYAN}repair${RESET}                       Check database integrity
  ${CYAN}sql${RESET} 'SELECT ...'             Run raw SQL against the archive DB(s)
  ${CYAN}sql${RESET} -                        Read SQL from stdin
  ${CYAN}status${RESET}                       Archive stats per workspace
  ${CYAN}doctor${RESET}                       Check config, deps, and DB health

${BOLD}EXAMPLES${RESET}
  slackslaw auth
  slackslaw sync --full
  slackslaw sync --channel "#ops" --since 7d
  slackslaw whatsnew
  slackslaw whatsnew --json | jq '.text'
  slackslaw search "incident P0" --after 30d
  slackslaw search "deploy" --channel "#infra" --json
  slackslaw search "staging" --slackmd
  slackslaw threads https://team.slack.com/archives/C0123/p1234567890123456
  slackslaw stats
  slackslaw export --channel "#general" > general.md
  slackslaw export --channel "#general" --format html > general.html
  slackslaw tail --interval 10 --channel "#ops"
  slackslaw context --since 24h | head -50
  ANTHROPIC_API_KEY=... slackslaw ask "what was discussed about staging?"
  slackslaw gc
  slackslaw repair
  slackslaw workspaces remove oldteam --delete-data

${BOLD}DATA${RESET}
  Archives  : ~/.slackslaw/workspaces/<workspace>/
  State     : ~/.slackslaw/state.json
  Logs      : ~/.slackslaw/slackslaw.log

${BOLD}ENVIRONMENT${RESET}
  SLACKSLAW_DIR      Override base data directory (default: ~/.slackslaw)
  NO_COLOR           Disable colour output
  ANTHROPIC_API_KEY  Required for 'ask' command (default provider)
  OPENAI_API_KEY     Required for 'ask --provider openai'

${BOLD}HOW SYNC WORKS${RESET}
  First sync (or --full) : slackdump archive -workspace <ws> -y -o <dir>
  Subsequent syncs       : slackdump resume -workspace <ws> <dir>
  In slackdump v4, -workspace and -y are per-subcommand flags and must
  come AFTER the subcommand name.

EOF
}

# ── Dispatch ──────────────────────────────────────────────────────────────────
main() {
  ensure_python3
  init_dirs

  local cmd="${1:-help}"
  shift 2>/dev/null || true

  case "$cmd" in
    auth)              cmd_auth       "$@" ;;
    workspaces|ws)
      # Handle subcommands: workspaces remove
      if [[ "${1:-}" == "remove" || "${1:-}" == "rm" ]]; then
        shift
        cmd_workspaces_remove "$@"
      elif [[ "${1:-}" == "add" ]]; then
        shift
        cmd_auth "$@"
      else
        cmd_workspaces "$@"
      fi
      ;;
    keywords|kw)       cmd_keywords   "$@" ;;
    sync)              cmd_sync       "$@" ;;
    whatsnew|new)      cmd_whatsnew   "$@" ;;
    unread)            cmd_unread     "$@" ;;
    search)            cmd_search     "$@" ;;
    sql)               cmd_sql        "$@" ;;
    status)            cmd_status     "$@" ;;
    stats)             cmd_stats      "$@" ;;
    threads|thread)    cmd_threads    "$@" ;;
    export)            cmd_export     "$@" ;;
    tail)              cmd_tail       "$@" ;;
    context)           cmd_context    "$@" ;;
    ask)               cmd_ask        "$@" ;;
    mcp)               cmd_mcp        "$@" ;;
    gc)                cmd_gc         "$@" ;;
    repair)            cmd_repair     "$@" ;;
    doctor)            cmd_doctor     "$@" ;;
    help|--help|-h)    cmd_help       ;;
    version|--version|-v)
      echo "slackslaw — the CLI for Slack (backed by slackdump)"
      command -v slackdump >/dev/null 2>&1 \
        && { slackdump --version 2>/dev/null || slackdump version 2>/dev/null || true; }
      ;;
    *)
      error "Unknown command: $cmd"
      echo ""
      cmd_help
      exit 1
      ;;
  esac
}

main "$@"
