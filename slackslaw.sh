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
#   slackslaw sync --full                   # full archive of all workspaces
#   slackslaw sync                          # incremental sync (new msgs only)
#   slackslaw whatsnew                      # print msgs since last sync
#   slackslaw whatsnew --json               # same, JSON output (one obj/line)
#   slackslaw search "expr"                 # FTS search across all workspaces
#   slackslaw search "expr" --workspace W   # limit to one workspace
#   slackslaw search "expr" --channel "#c"  # limit to a channel
#   slackslaw search "expr" --author user   # limit to a user
#   slackslaw search "expr" --limit 50      # max results (default 20)
#   slackslaw search "expr" --json          # JSON output (one obj/line)
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
#   - `resume -workspace <ws> -y <dir>`     — incremental from checkpoint
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
      local cmd="slackdump resume -workspace \"$ws\" -y \"$ws_dir\""
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

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --json)        as_json=true ;;
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

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --json)        as_json=true ;;
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
      --workspace|-w)    shift; target_ws="$1" ;;
      --channel|-c)      shift; channel_filter="${1#\#}" ;;
      --author|-a)       shift; author_filter="$1" ;;
      --limit|-l)        shift; limit="$1" ;;
      --mine|-m)         mine_filter=true ;;
      --newest-first)    sort_order="DESC" ;;
      --after)           shift; after_ts=$(parse_date_to_ts "$1") ;;
      --before)          shift; before_ts=$(parse_date_to_ts "$1") ;;
      --format)          shift; format_mode="$1" ;;
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
      sql+=" AND LOWER(COALESCE(c.NAME,'')) = LOWER('${channel_filter}')"
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
      elif [[ "$format_mode" == "slack" ]]; then
        # Raw Slack mrkdwn output — skip prettify
        echo "$rows_json" | python3 -c "
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
    text = r.get('text_snippet', '') or r.get('full_text', '') or r.get('TXT', '') or ''
    print(f'{dt}  #{ch}  {author}  {text}')
"
      else
        echo -e "\n${BOLD}${CYAN}Workspace: ${ws}${RESET}"
        format_rows "$db" "$user_table" "detail" "$rows_json"
      fi
    fi

  done <<< "$workspaces"

  if ! $any_results; then
    if $as_json; then echo "[]"; else info "No results for: ${query}"; fi
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
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --json) as_json=true ;;
      *)      warn "Unknown option: $1" ;;
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
  ${CYAN}keywords${RESET}   (alias: kw)      List configured channel keywords
  ${CYAN}keywords${RESET} add <word>          Add a keyword (used by --mine filter)
  ${CYAN}keywords${RESET} remove <word>       Remove a keyword
  ${CYAN}sync${RESET} [--full]                Sync workspaces (incremental by default)
  ${CYAN}sync${RESET} --workspace <n>         Sync a single workspace
  ${CYAN}whatsnew${RESET}    (alias: new)     Messages received since the last sync
  ${CYAN}whatsnew${RESET} --mine              Only messages relevant to me (+ keywords)
  ${CYAN}whatsnew${RESET} --json              Same, as JSONL (one object per line)
  ${CYAN}unread${RESET}                       Show messages marked as unread
  ${CYAN}unread${RESET} --json                Same, as JSONL (one object per line)
  ${CYAN}search${RESET} "expr"                Full-text search across all workspaces
  ${CYAN}search${RESET} "expr" --workspace W  Limit to one workspace
  ${CYAN}search${RESET} "expr" --channel C    Limit to channel (# prefix optional)
  ${CYAN}search${RESET} "expr" --author A     Limit to author name
  ${CYAN}search${RESET} "expr" --mine         Only threads I'm in (participated, mentioned, following, starred)
  ${CYAN}search${RESET} "expr" --newest-first Show newest results first (default: oldest first)
  ${CYAN}search${RESET} "expr" --limit N      Max results (default: 20)
  ${CYAN}search${RESET} "expr" --json         JSONL output
  ${CYAN}sql${RESET} 'SELECT ...'             Run raw SQL against the archive DB(s)
  ${CYAN}sql${RESET} -                        Read SQL from stdin
  ${CYAN}status${RESET}                       Archive stats per workspace
  ${CYAN}doctor${RESET}                       Check config, deps, and DB health

${BOLD}EXAMPLES${RESET}
  slackslaw auth
  slackslaw sync --full
  slackslaw sync
  slackslaw whatsnew
  slackslaw whatsnew --json | jq '.text'
  slackslaw search "incident P0"
  slackslaw search "deploy" --channel "#infra" --json
  slackslaw search "code review" --author petra --limit 50
  slackslaw sql 'SELECT name, COUNT(*) FROM channels GROUP BY name ORDER BY 2 DESC'
  echo 'SELECT ts, text FROM messages LIMIT 5' | slackslaw sql -

${BOLD}DATA${RESET}
  Archives  : ~/.slackslaw/workspaces/<workspace>/
  State     : ~/.slackslaw/state.json
  Logs      : ~/.slackslaw/slackslaw.log

${BOLD}ENVIRONMENT${RESET}
  SLACKSLAW_DIR   Override base data directory (default: ~/.slackslaw)
  NO_COLOR        Disable colour output

${BOLD}HOW SYNC WORKS${RESET}
  First sync (or --full) : slackdump archive -workspace <ws> -y -o <dir>
  Subsequent syncs       : slackdump resume -workspace <ws> -y <dir>
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
    workspaces|ws)     cmd_workspaces "$@" ;;
    keywords|kw)       cmd_keywords   "$@" ;;
    sync)              cmd_sync       "$@" ;;
    whatsnew|new)      cmd_whatsnew   "$@" ;;
    unread)            cmd_unread     "$@" ;;
    search)            cmd_search     "$@" ;;
    sql)               cmd_sql        "$@" ;;
    status)            cmd_status     "$@" ;;
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
