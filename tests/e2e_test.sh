#!/usr/bin/env bash
# =============================================================================
# e2e_test.sh — End-to-end tests for slackslaw
#
# Runs slackslaw commands against a seeded test environment (temp SLACKSLAW_DIR
# with pre-populated SQLite databases). No real Slack workspace needed.
#
# Usage:
#   ./tests/e2e_test.sh           # run all tests
#   ./tests/e2e_test.sh -v        # verbose (show command output)
#   ./tests/e2e_test.sh -f test_  # filter: only run tests matching pattern
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SLACKSLAW="$PROJECT_DIR/slackslaw.sh"

# ── Test framework ──────────────────────────────────────────────────────────

PASS=0
FAIL=0
SKIP=0
VERBOSE=false
FILTER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -v|--verbose) VERBOSE=true ;;
    -f|--filter)  shift; FILTER="$1" ;;
    *)            echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

RED=$'\033[0;31m'
GREEN=$'\033[0;32m'
YELLOW=$'\033[0;33m'
DIM=$'\033[2m'
BOLD=$'\033[1m'
RESET=$'\033[0m'

pass() { PASS=$((PASS + 1)); echo -e "  ${GREEN}✓${RESET} $1"; }
fail() { FAIL=$((FAIL + 1)); echo -e "  ${RED}✗${RESET} $1"; echo -e "    ${DIM}$2${RESET}"; }
skip() { SKIP=$((SKIP + 1)); echo -e "  ${YELLOW}○${RESET} $1 ${DIM}(skipped)${RESET}"; }

# Run a slackslaw command, capture stdout+stderr and exit code.
# Usage: run_slackslaw <args...>
# Sets: $OUTPUT (stdout+stderr), $EXIT_CODE
run_slackslaw() {
  set +e
  OUTPUT=$(NO_COLOR=1 SLACKSLAW_DIR="$TEST_DIR" bash "$SLACKSLAW" "$@" 2>&1)
  EXIT_CODE=$?
  set -e
  if $VERBOSE; then echo -e "    ${DIM}$ slackslaw $*${RESET}\n    ${DIM}${OUTPUT}${RESET}"; fi
}

# Assert that $OUTPUT contains a string
assert_contains() {
  local needle="$1" msg="${2:-output contains '$1'}"
  if echo "$OUTPUT" | grep -qF "$needle"; then
    pass "$msg"
  else
    fail "$msg" "Expected to find '$needle' in output:\n$OUTPUT"
  fi
}

# Assert that $OUTPUT does NOT contain a string
assert_not_contains() {
  local needle="$1" msg="${2:-output does not contain '$1'}"
  if echo "$OUTPUT" | grep -qF "$needle"; then
    fail "$msg" "Did NOT expect to find '$needle' in output:\n$OUTPUT"
  else
    pass "$msg"
  fi
}

# Assert that $OUTPUT matches a regex
assert_matches() {
  local pattern="$1" msg="${2:-output matches '$1'}"
  if echo "$OUTPUT" | grep -qE "$pattern"; then
    pass "$msg"
  else
    fail "$msg" "Expected output to match /$pattern/:\n$OUTPUT"
  fi
}

# Assert exit code
assert_exit() {
  local expected="$1" msg="${2:-exit code is $1}"
  if [[ "$EXIT_CODE" -eq "$expected" ]]; then
    pass "$msg"
  else
    fail "$msg" "Expected exit $expected, got $EXIT_CODE"
  fi
}

# Assert $OUTPUT is valid JSON (each line is a JSON object)
assert_json() {
  local msg="${1:-output is valid JSON}"
  if echo "$OUTPUT" | python3 -c "import json,sys; [json.loads(l) for l in sys.stdin if l.strip()]" 2>/dev/null; then
    pass "$msg"
  else
    fail "$msg" "Output is not valid JSON:\n$OUTPUT"
  fi
}

# Assert $OUTPUT is a valid JSON array
assert_json_array() {
  local msg="${1:-output is a valid JSON array}"
  if echo "$OUTPUT" | python3 -c "import json,sys; d=json.load(sys.stdin); assert isinstance(d,list)" 2>/dev/null; then
    pass "$msg"
  else
    fail "$msg" "Output is not a valid JSON array:\n$OUTPUT"
  fi
}

# ── Test environment setup ──────────────────────────────────────────────────

TEST_DIR=""
TEST_WS="testteam.slack.com"
TEST_WS_SHORT="testteam"

setup_test_env() {
  TEST_DIR=$(mktemp -d)

  local ws_dir="$TEST_DIR/workspaces/$TEST_WS_SHORT"
  mkdir -p "$ws_dir"

  # Create config with one workspace
  cat > "$TEST_DIR/config.json" <<EOF
{
  "workspaces": ["$TEST_WS"],
  "keywords": ["deploy", "incident"]
}
EOF

  # Create state file with a sync timestamp
  cat > "$TEST_DIR/state.json" <<EOF
{
  "${TEST_WS}_last_sync": "2025-03-01 12:00:00",
  "${TEST_WS}_last_sync_ts": "1700000000"
}
EOF

  touch "$TEST_DIR/slackslaw.log"

  # Create a test SQLite database matching slackdump v4 schema
  local db="$ws_dir/archive.db"
  sqlite3 "$db" <<'SQL'
    -- slackdump v4 schema
    CREATE TABLE WORKSPACE (
      ID TEXT PRIMARY KEY,
      USER_ID TEXT,
      TEAM_NAME TEXT
    );
    INSERT INTO WORKSPACE VALUES ('T01TESTWS', 'U01MYSELF', 'TestTeam');

    CREATE TABLE CHANNEL (
      ID TEXT,
      NAME TEXT,
      DATA TEXT
    );
    INSERT INTO CHANNEL VALUES ('C01GENERAL', 'general', '{"is_channel":true,"last_read":"1700000050.000000"}');
    INSERT INTO CHANNEL VALUES ('C02OPS',     'ops',     '{"is_channel":true,"last_read":"1700000000.000000"}');
    INSERT INTO CHANNEL VALUES ('C03RANDOM',  'random',  '{"is_channel":true,"last_read":"0000000000.000000"}');
    INSERT INTO CHANNEL VALUES ('D01DM',      '',        '{"is_im":true,"user":"U02BOB"}');

    CREATE TABLE S_USER (
      ID TEXT,
      USERNAME TEXT
    );
    INSERT INTO S_USER VALUES ('U01MYSELF', 'alice');
    INSERT INTO S_USER VALUES ('U02BOB',    'bob');
    INSERT INTO S_USER VALUES ('U03CAROL',  'carol');
    INSERT INTO S_USER VALUES ('U04DAVE',   'dave');

    CREATE TABLE MESSAGE (
      TS TEXT,
      CHANNEL_ID TEXT,
      TXT TEXT,
      THREAD_TS TEXT,
      DATA TEXT,
      NUM_FILES INTEGER DEFAULT 0
    );

    -- Messages in #general
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000010.000000', 'C01GENERAL', 'Hello everyone!', NULL, '{"user":"U02BOB","type":"message"}');
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000020.000000', 'C01GENERAL', 'Hey <@U02BOB>, welcome!', NULL, '{"user":"U01MYSELF","type":"message"}');
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000030.000000', 'C01GENERAL', 'Starting deploy to staging', NULL, '{"user":"U03CAROL","type":"message"}');
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000060.000000', 'C01GENERAL', 'Deploy complete! All green.', NULL, '{"user":"U03CAROL","type":"message"}');

    -- Messages in #ops (thread example)
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000040.000000', 'C02OPS', 'P0 incident: database timeout', NULL, '{"user":"U04DAVE","type":"message"}');
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000041.000000', 'C02OPS', 'Looking into it now', '1700000040.000000', '{"user":"U01MYSELF","type":"message"}');
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000042.000000', 'C02OPS', 'Resolved - connection pool was exhausted', '1700000040.000000', '{"user":"U04DAVE","type":"message"}');

    -- Message in DM
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000050.000000', 'D01DM', 'Hey Alice, quick question', NULL, '{"user":"U02BOB","type":"message"}');

    -- Messages in #random
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000070.000000', 'C03RANDOM', 'Anyone up for lunch?', NULL, '{"user":"U02BOB","type":"message"}');
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1700000080.000000', 'C03RANDOM', 'Sure! <@U02BOB>', NULL, '{"user":"U03CAROL","type":"message"}');

    -- Older message (before sync baseline) for whatsnew testing
    INSERT INTO MESSAGE (TS, CHANNEL_ID, TXT, THREAD_TS, DATA) VALUES ('1699999900.000000', 'C01GENERAL', 'Old message before sync', NULL, '{"user":"U02BOB","type":"message"}');
SQL
}

teardown_test_env() {
  [[ -n "$TEST_DIR" && -d "$TEST_DIR" ]] && rm -rf "$TEST_DIR"
}

# ── Test definitions ────────────────────────────────────────────────────────

test_help() {
  echo -e "\n${BOLD}help / usage${RESET}"

  run_slackslaw help
  assert_exit 0 "help exits 0"
  assert_contains "USAGE" "help shows usage section"
  assert_contains "COMMANDS" "help shows commands section"
  assert_contains "search" "help mentions search command"
  assert_contains "sync" "help mentions sync command"

  run_slackslaw --help
  assert_exit 0 "--help exits 0"
  assert_contains "USAGE" "--help shows usage"
}

test_version() {
  echo -e "\n${BOLD}version${RESET}"

  run_slackslaw version
  assert_contains "slackslaw" "version output mentions slackslaw"
}

test_unknown_command() {
  echo -e "\n${BOLD}unknown command${RESET}"

  run_slackslaw nosuchcommand
  assert_exit 1 "unknown command exits 1"
  assert_contains "Unknown command" "unknown command shows error"
}

test_doctor() {
  echo -e "\n${BOLD}doctor${RESET}"

  run_slackslaw doctor
  assert_exit 0 "doctor exits 0"
  assert_contains "slackslaw Doctor" "doctor shows header"
  assert_contains "sqlite3 installed" "doctor checks sqlite3"
  assert_contains "python3 installed" "doctor checks python3"
  assert_contains "Config dir exists" "doctor checks config dir"
  assert_contains "State file exists" "doctor checks state file"
  assert_contains "Archive DB for" "doctor finds test archive DB"
  assert_contains "$TEST_WS" "doctor lists test workspace"
}

test_workspaces_list() {
  echo -e "\n${BOLD}workspaces list${RESET}"

  run_slackslaw workspaces
  assert_exit 0 "workspaces exits 0"
  assert_contains "$TEST_WS" "workspaces lists test workspace"
  assert_contains "messages" "workspaces shows message count"
  assert_contains "last sync" "workspaces shows last sync"
}

test_workspaces_empty() {
  echo -e "\n${BOLD}workspaces (empty)${RESET}"

  # Override config to have no workspaces
  local orig_config
  orig_config=$(cat "$TEST_DIR/config.json")
  echo '{"workspaces":[]}' > "$TEST_DIR/config.json"

  run_slackslaw workspaces
  assert_exit 0 "workspaces (empty) exits 0"
  assert_contains "No workspaces configured" "shows no-workspaces message"

  echo "$orig_config" > "$TEST_DIR/config.json"
}

test_keywords_list() {
  echo -e "\n${BOLD}keywords list${RESET}"

  run_slackslaw keywords list
  assert_exit 0 "keywords list exits 0"
  assert_contains "deploy" "keywords shows 'deploy'"
  assert_contains "incident" "keywords shows 'incident'"
}

test_keywords_add_remove() {
  echo -e "\n${BOLD}keywords add/remove${RESET}"

  run_slackslaw keywords add "outage"
  assert_exit 0 "keywords add exits 0"
  assert_contains "Added keyword" "keywords add confirms"

  run_slackslaw keywords list
  assert_contains "outage" "newly added keyword appears in list"

  run_slackslaw keywords remove "outage"
  assert_exit 0 "keywords remove exits 0"
  assert_contains "Removed keyword" "keywords remove confirms"

  run_slackslaw keywords list
  assert_not_contains "outage" "removed keyword no longer in list"
}

test_keywords_empty() {
  echo -e "\n${BOLD}keywords (empty)${RESET}"

  local orig_config
  orig_config=$(cat "$TEST_DIR/config.json")
  python3 -c "
import json
d = json.load(open('$TEST_DIR/config.json'))
d['keywords'] = []
json.dump(d, open('$TEST_DIR/config.json', 'w'), indent=2)
"

  run_slackslaw keywords list
  assert_exit 0 "keywords (empty) exits 0"
  assert_contains "No keywords configured" "shows no-keywords message"

  echo "$orig_config" > "$TEST_DIR/config.json"
}

test_status() {
  echo -e "\n${BOLD}status${RESET}"

  run_slackslaw status
  assert_exit 0 "status exits 0"
  assert_contains "Slack Archive Status" "status shows header"
  assert_contains "$TEST_WS" "status lists workspace"
  assert_contains "Messages" "status shows message count"
  assert_contains "Channels" "status shows channel count"
  assert_contains "Users" "status shows user count"
  assert_contains "DB size" "status shows DB size"
  assert_contains "Last sync" "status shows last sync"
}

test_search_basic() {
  echo -e "\n${BOLD}search (basic)${RESET}"

  run_slackslaw search "deploy"
  assert_exit 0 "search exits 0"
  assert_contains "deploy" "search finds deploy messages"
  assert_contains "carol" "search shows author"
}

test_search_no_results() {
  echo -e "\n${BOLD}search (no results)${RESET}"

  run_slackslaw search "xyznonexistent"
  assert_exit 0 "search with no results exits 0"
  assert_contains "No results" "search shows no-results message"
}

test_search_json() {
  echo -e "\n${BOLD}search --json${RESET}"

  run_slackslaw search "deploy" --json
  assert_exit 0 "search --json exits 0"
  assert_json "search --json outputs valid JSONL"
  assert_contains "deploy" "search JSON contains deploy text"
  assert_contains "datetime" "search JSON includes datetime field"
  assert_contains "workspace" "search JSON includes workspace field"
}

test_search_channel_filter() {
  echo -e "\n${BOLD}search --channel${RESET}"

  run_slackslaw search "deploy" --channel "#general" --json
  assert_exit 0 "search --channel exits 0"
  assert_contains "general" "search filtered to general channel"

  # Searching in ops should NOT find deploy messages
  run_slackslaw search "deploy" --channel "#ops" --json
  assert_contains "[]" "search in wrong channel returns empty"
}

test_search_author_filter() {
  echo -e "\n${BOLD}search --author${RESET}"

  run_slackslaw search "deploy" --author "carol" --json
  assert_exit 0 "search --author exits 0"
  assert_contains "carol" "search filtered to carol"
}

test_search_limit() {
  echo -e "\n${BOLD}search --limit${RESET}"

  run_slackslaw search "deploy" --limit 1 --json
  assert_exit 0 "search --limit exits 0"
  # Should return only 1 result line
  local count
  count=$(echo "$OUTPUT" | grep -c '{' || true)
  if [[ "$count" -eq 1 ]]; then
    pass "search --limit 1 returns exactly 1 result"
  else
    fail "search --limit 1 returns exactly 1 result" "Got $count results"
  fi
}

test_search_newest_first() {
  echo -e "\n${BOLD}search --newest-first${RESET}"

  run_slackslaw search "deploy" --newest-first --json
  assert_exit 0 "search --newest-first exits 0"

  # Check that first result has a later timestamp than last
  local check_result
  check_result=$(echo "$OUTPUT" | python3 -c "
import json, sys
lines = [l.strip() for l in sys.stdin if l.strip()]
if len(lines) < 2:
    print('ok')  # only one result, trivially sorted
else:
    first = json.loads(lines[0])
    last = json.loads(lines[-1])
    ft = float(first.get('ts', 0))
    lt = float(last.get('ts', 0))
    print('ok' if ft >= lt else f'fail: {ft} < {lt}')
" 2>/dev/null || echo "error")
  if [[ "$check_result" == "ok" ]]; then
    pass "search --newest-first returns newest first"
  else
    fail "search --newest-first returns newest first" "$check_result"
  fi
}

test_search_slackmd() {
  echo -e "\n${BOLD}search --slackmd${RESET}"

  run_slackslaw search "deploy" --slackmd
  assert_exit 0 "search --slackmd exits 0"
  assert_contains "carol" "slackmd output includes author"
  assert_contains "#general" "slackmd output includes channel"
}

test_search_format_json() {
  echo -e "\n${BOLD}search --format json${RESET}"

  run_slackslaw search "deploy" --format json
  assert_exit 0 "search --format json exits 0"
  assert_json "search --format json outputs valid JSONL"
}

test_search_no_query() {
  echo -e "\n${BOLD}search (no query)${RESET}"

  run_slackslaw search
  assert_exit 1 "search without query exits 1"
  assert_contains "Usage" "search without query shows usage"
}

test_whatsnew() {
  echo -e "\n${BOLD}whatsnew${RESET}"

  run_slackslaw whatsnew
  assert_exit 0 "whatsnew exits 0"
  # Messages with ts > 1700000000 should appear (our sync baseline)
  assert_contains "alice" "whatsnew shows messages from alice"
}

test_whatsnew_json() {
  echo -e "\n${BOLD}whatsnew --json${RESET}"

  run_slackslaw whatsnew --json
  assert_exit 0 "whatsnew --json exits 0"
  assert_json "whatsnew --json outputs valid JSONL"
  assert_contains "datetime" "whatsnew JSON includes datetime"
}

test_whatsnew_no_new() {
  echo -e "\n${BOLD}whatsnew (nothing new)${RESET}"

  # Set sync timestamp far in the future
  local orig_state
  orig_state=$(cat "$TEST_DIR/state.json")
  python3 -c "
import json
d = json.load(open('$TEST_DIR/state.json'))
d['${TEST_WS}_last_sync_ts'] = '9999999999'
json.dump(d, open('$TEST_DIR/state.json', 'w'), indent=2)
"

  run_slackslaw whatsnew
  assert_contains "No new messages" "whatsnew shows no-new-messages"

  echo "$orig_state" > "$TEST_DIR/state.json"
}

test_whatsnew_slackmd() {
  echo -e "\n${BOLD}whatsnew --slackmd${RESET}"

  run_slackslaw whatsnew --slackmd
  assert_exit 0 "whatsnew --slackmd exits 0"
  # Should contain raw output without table formatting
  assert_matches "[0-9]{4}-[0-9]{2}-[0-9]{2}" "slackmd output includes dates"
}

test_unread() {
  echo -e "\n${BOLD}unread${RESET}"

  run_slackslaw unread
  assert_exit 0 "unread exits 0"
  # Messages in #general after last_read (1700000050) should appear
  assert_contains "Deploy complete" "unread shows message after last_read"
}

test_unread_json() {
  echo -e "\n${BOLD}unread --json${RESET}"

  run_slackslaw unread --json
  assert_exit 0 "unread --json exits 0"
  assert_json "unread --json outputs valid JSONL"
}

test_sql() {
  echo -e "\n${BOLD}sql${RESET}"

  run_slackslaw sql "SELECT COUNT(*) FROM MESSAGE;"
  assert_exit 0 "sql exits 0"
  assert_contains "11" "sql returns correct message count"
}

test_sql_select_users() {
  echo -e "\n${BOLD}sql (users)${RESET}"

  run_slackslaw sql "SELECT USERNAME FROM S_USER ORDER BY USERNAME;"
  assert_exit 0 "sql users exits 0"
  assert_contains "alice" "sql returns alice"
  assert_contains "bob" "sql returns bob"
  assert_contains "carol" "sql returns carol"
  assert_contains "dave" "sql returns dave"
}

test_sql_stdin() {
  echo -e "\n${BOLD}sql from stdin${RESET}"

  set +e
  OUTPUT=$(echo "SELECT COUNT(*) FROM CHANNEL;" | NO_COLOR=1 SLACKSLAW_DIR="$TEST_DIR" bash "$SLACKSLAW" sql - 2>&1)
  EXIT_CODE=$?
  set -e

  assert_exit 0 "sql stdin exits 0"
  assert_contains "4" "sql stdin returns correct channel count"
}

test_sql_no_query() {
  echo -e "\n${BOLD}sql (no query)${RESET}"

  run_slackslaw sql ""
  assert_exit 1 "sql without query exits 1"
  assert_contains "Usage" "sql without query shows usage"
}

test_stats() {
  echo -e "\n${BOLD}stats${RESET}"

  run_slackslaw stats
  assert_exit 0 "stats exits 0"
  assert_contains "Stats:" "stats shows header"
  assert_contains "Total messages" "stats shows total messages"
  assert_contains "Channels" "stats shows channels"
  assert_contains "Users" "stats shows users"
  assert_contains "Top 10 Message Senders" "stats shows top senders"
  assert_contains "Top 10 Busiest Channels" "stats shows busiest channels"
  assert_contains "Most Active Hours" "stats shows active hours"
}

test_threads() {
  echo -e "\n${BOLD}threads${RESET}"

  # Thread parent ts = 1700000040.000000, URL-encoded: p1700000040000000
  run_slackslaw threads "https://testteam.slack.com/archives/C02OPS/p1700000040000000"
  assert_exit 0 "threads exits 0"
  assert_contains "database timeout" "threads shows thread parent"
  assert_contains "Looking into it" "threads shows thread reply"
  assert_contains "Resolved" "threads shows second reply"
}

test_threads_json() {
  echo -e "\n${BOLD}threads --json${RESET}"

  run_slackslaw threads "https://testteam.slack.com/archives/C02OPS/p1700000040000000" --json
  assert_exit 0 "threads --json exits 0"
  assert_json "threads --json outputs valid JSONL"
  assert_contains "database timeout" "threads JSON contains parent message"
}

test_threads_bad_url() {
  echo -e "\n${BOLD}threads (bad URL)${RESET}"

  run_slackslaw threads "https://example.com/notaslackurl"
  assert_exit 1 "threads with bad URL exits 1"
  assert_contains "Could not parse" "threads bad URL shows error"
}

test_threads_no_url() {
  echo -e "\n${BOLD}threads (no URL)${RESET}"

  run_slackslaw threads
  assert_exit 1 "threads without URL exits 1"
  assert_contains "Usage" "threads without URL shows usage"
}

test_export_markdown() {
  echo -e "\n${BOLD}export --format md${RESET}"

  run_slackslaw export --channel "#general"
  assert_contains "## #general" "export md has channel header"
  assert_contains "bob" "export md includes author"
  assert_contains "Hello everyone" "export md includes message text"
}

test_export_html() {
  echo -e "\n${BOLD}export --format html${RESET}"

  run_slackslaw export --channel "#general" --format html
  assert_contains "<!DOCTYPE html>" "export html has doctype"
  assert_contains "<title>#general</title>" "export html has title"
  assert_contains "Hello everyone" "export html includes message text"
  assert_contains "bob" "export html includes author"
}

test_export_to_file() {
  echo -e "\n${BOLD}export --output file${RESET}"

  local outfile="$TEST_DIR/test_export.md"
  run_slackslaw export --channel "#general" --output "$outfile"
  assert_exit 0 "export to file exits 0"

  if [[ -f "$outfile" ]]; then
    pass "export creates output file"
    if grep -q "Hello everyone" "$outfile"; then
      pass "export file contains message text"
    else
      fail "export file contains message text" "File content: $(cat "$outfile")"
    fi
  else
    fail "export creates output file" "File not found: $outfile"
  fi
}

test_export_no_channel() {
  echo -e "\n${BOLD}export (no channel)${RESET}"

  run_slackslaw export
  assert_exit 1 "export without channel exits 1"
  assert_contains "Usage" "export without channel shows usage"
}

test_context() {
  echo -e "\n${BOLD}context${RESET}"

  run_slackslaw context --since 9999d
  assert_exit 0 "context exits 0"
  assert_contains "## #general" "context includes general channel"
  assert_contains "## #ops" "context includes ops channel"
  assert_contains "Hello everyone" "context includes message text"
  assert_contains "bob" "context includes author"
}

test_context_channel_filter() {
  echo -e "\n${BOLD}context --channel${RESET}"

  run_slackslaw context --since 9999d --channel "#ops"
  assert_exit 0 "context --channel exits 0"
  assert_contains "ops" "context filtered to ops channel"
}

test_gc() {
  echo -e "\n${BOLD}gc${RESET}"

  run_slackslaw gc
  assert_exit 0 "gc exits 0"
  assert_contains "Garbage Collection" "gc shows header"
  assert_contains "Vacuuming" "gc shows vacuum progress"
  assert_contains "Done" "gc shows completion"
}

test_repair() {
  echo -e "\n${BOLD}repair${RESET}"

  run_slackslaw repair
  assert_exit 0 "repair exits 0"
  assert_contains "Database Integrity Check" "repair shows header"
  assert_contains "database OK" "repair reports OK"
}

test_mention_prettify() {
  echo -e "\n${BOLD}mention prettification${RESET}"

  # Search for the message containing <@U02BOB> and check it gets prettified
  run_slackslaw search "welcome"
  assert_exit 0 "search for mention exits 0"
  assert_contains "@bob" "mention <@U02BOB> prettified to @bob"
}

test_workspaces_remove_nonexistent() {
  echo -e "\n${BOLD}workspaces remove (nonexistent)${RESET}"

  run_slackslaw workspaces remove "nosuchworkspace"
  assert_exit 1 "workspaces remove nonexistent exits 1"
  assert_contains "not found" "workspaces remove nonexistent shows error"
}

test_workspaces_remove_existing() {
  echo -e "\n${BOLD}workspaces remove (existing)${RESET}"

  # Add a workspace then remove it
  local orig_config
  orig_config=$(cat "$TEST_DIR/config.json")

  python3 -c "
import json
d = json.load(open('$TEST_DIR/config.json'))
d['workspaces'].append('removeme.slack.com')
json.dump(d, open('$TEST_DIR/config.json', 'w'), indent=2)
"

  run_slackslaw workspaces remove "removeme"
  assert_exit 0 "workspaces remove existing exits 0"
  assert_contains "Removed workspace" "workspaces remove confirms removal"

  echo "$orig_config" > "$TEST_DIR/config.json"
}

test_multi_format_flag() {
  echo -e "\n${BOLD}--format flag variants${RESET}"

  run_slackslaw search "deploy" --format json
  assert_exit 0 "search --format json exits 0"
  assert_json "search --format json outputs JSONL"

  run_slackslaw search "deploy" --format slackmd
  assert_exit 0 "search --format slackmd exits 0"
  assert_contains "#general" "search --format slackmd shows channel"
}

test_search_after_filter() {
  echo -e "\n${BOLD}search --after${RESET}"

  # All our messages have ts around 1700000000-1700000080
  # Search after a date far in the past — should find results
  run_slackslaw search "deploy" --after "2020-01-01" --json
  assert_exit 0 "search --after past date exits 0"
  assert_contains "deploy" "search --after past date finds results"

  # Search after a date far in the future — should find nothing
  run_slackslaw search "deploy" --after "2099-01-01" --json
  assert_contains "[]" "search --after future date finds nothing"
}

test_search_before_filter() {
  echo -e "\n${BOLD}search --before${RESET}"

  # Search before a date far in the future — should find results
  run_slackslaw search "deploy" --before "2099-01-01" --json
  assert_exit 0 "search --before future date exits 0"
  assert_contains "deploy" "search --before future finds results"

  # Search before a date far in the past — should find nothing
  run_slackslaw search "deploy" --before "2020-01-01" --json
  assert_contains "[]" "search --before past date finds nothing"
}

test_workspace_dir_shortname() {
  echo -e "\n${BOLD}workspace directory uses short name${RESET}"

  # The workspace dir should strip .slack.com
  if [[ -d "$TEST_DIR/workspaces/$TEST_WS_SHORT" ]]; then
    pass "workspace dir uses short name (without .slack.com)"
  else
    fail "workspace dir uses short name" "Expected dir: $TEST_DIR/workspaces/$TEST_WS_SHORT"
  fi
}

test_search_workspace_filter() {
  echo -e "\n${BOLD}search --workspace${RESET}"

  run_slackslaw search "deploy" --workspace "$TEST_WS" --json
  assert_exit 0 "search --workspace exits 0"
  assert_contains "deploy" "search --workspace finds results in correct workspace"
}

test_whatsnew_limit() {
  echo -e "\n${BOLD}whatsnew --limit${RESET}"

  run_slackslaw whatsnew --limit 2 --json
  assert_exit 0 "whatsnew --limit exits 0"
  local count
  count=$(echo "$OUTPUT" | grep -c '{' || true)
  if [[ "$count" -le 2 ]]; then
    pass "whatsnew --limit 2 returns at most 2 results"
  else
    fail "whatsnew --limit 2 returns at most 2 results" "Got $count results"
  fi
}

test_export_nonexistent_channel() {
  echo -e "\n${BOLD}export (nonexistent channel)${RESET}"

  run_slackslaw export --channel "#nosuchch"
  # Should produce no meaningful output (no crash)
  assert_not_contains "Traceback" "export nonexistent channel does not crash"
}

test_threads_wrong_channel() {
  echo -e "\n${BOLD}threads (channel not found)${RESET}"

  run_slackslaw threads "https://testteam.slack.com/archives/CNOTHERE/p1700000040000000"
  assert_exit 0 "threads with unknown channel exits 0"
  assert_contains "Thread not found" "threads shows not-found message"
}

# ── Run all tests ───────────────────────────────────────────────────────────

run_tests() {
  echo -e "\n${BOLD}═══ slackslaw e2e tests ═══${RESET}\n"

  setup_test_env
  trap teardown_test_env EXIT

  # Collect all test functions
  local tests
  tests=$(declare -F | awk '{print $3}' | grep '^test_' | sort)

  for t in $tests; do
    if [[ -n "$FILTER" ]] && ! echo "$t" | grep -q "$FILTER"; then
      continue
    fi
    "$t"
  done

  echo -e "\n${BOLD}═══ Results ═══${RESET}"
  echo -e "  ${GREEN}$PASS passed${RESET}  ${RED}$FAIL failed${RESET}  ${YELLOW}$SKIP skipped${RESET}"
  echo ""

  [[ $FAIL -eq 0 ]]
}

run_tests
