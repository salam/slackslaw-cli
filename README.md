# slackslaw

A CLI for Slack that mirrors your workspaces into local SQLite for offline search, agent-friendly querying, and incremental sync. Powered by [slackdump](https://github.com/rusq/slackdump).

## Features

- **Multi-workspace** — manage and sync multiple Slack workspaces
- **Incremental sync** — only fetch new messages after the first full archive
- **Full-text search** — search across all workspaces, filter by channel, author, or workspace
- **What's new** — see messages since your last sync at a glance
- **Unread view** — catch up on what you missed
- **Raw SQL** — query the local SQLite databases directly
- **JSON output** — pipe results into other tools or LLM agents
- **No jq required** — only needs `bash`, `python3`, and `sqlite3`

## Requirements

- **bash** 4+
- **python3**
- **sqlite3**
- **[slackdump](https://github.com/rusq/slackdump)** v4

## Installation

```bash
# Clone the repo
git clone https://github.com/your-username/slackslaw.git
cd slackslaw

# Make it executable (already is by default)
chmod +x slackslaw.sh

# Symlink into your PATH
ln -s "$PWD/slackslaw.sh" /usr/local/bin/slackslaw
```

## Quick start

```bash
# Check that dependencies are installed
slackslaw doctor

# Authenticate a Slack workspace
slackslaw auth

# Full sync (first time)
slackslaw sync --full

# Incremental sync (subsequent runs)
slackslaw sync

# See what's new since your last sync
slackslaw whatsnew

# Search across all workspaces
slackslaw search "quarterly roadmap"

# Search with filters
slackslaw search "deploy" --channel "#engineering" --author alice --limit 50

# JSON output for piping
slackslaw search "bug report" --json

# Raw SQL
slackslaw sql 'SELECT * FROM messages LIMIT 10'

# Archive stats
slackslaw status
```

## Commands

| Command | Description |
|---------|-------------|
| `auth` | Add or re-authenticate a workspace |
| `workspaces` | List configured workspaces |
| `sync --full` | Full archive of all workspaces |
| `sync` | Incremental sync (new messages only) |
| `whatsnew` | Print messages since last sync |
| `unread` | Show unread messages |
| `search "expr"` | Full-text search across all workspaces |
| `sql 'SELECT ...'` | Raw SQL against local databases |
| `status` | Archive stats per workspace |
| `doctor` | Check dependencies and configuration |

## Data storage

All data lives in `~/.slackslaw/` (override with `SLACKSLAW_DIR`):

```
~/.slackslaw/
  workspaces/<n>/      — slackdump archive directory per workspace
  workspaces/<n>/*.db  — SQLite database (auto-discovered)
  state.json           — last-sync timestamps
  config.json          — workspace list
```

## FAQ

### How is slackslaw different from slackdump?

[slackdump](https://github.com/rusq/slackdump) is the low-level Go tool that handles Slack API authentication, message downloading, and archive resumption. slackslaw is a bash wrapper that adds a higher-level UX on top:

- **Multi-workspace management** — slackdump operates on one workspace at a time; slackslaw tracks and syncs all of them together.
- **Incremental sync logic** — automatically decides between full archive and incremental resume per workspace based on stored timestamps.
- **Cross-workspace search** — queries across all workspace SQLite databases with filtering by channel, author, or workspace, and prettifies the output (resolving user IDs to names, formatting channels, etc.).
- **`whatsnew` / `unread`** — convenience commands with a `--mine` filter that surfaces messages where you're the author, mentioned, in a thread you participated in, or matching configured keywords.
- **Pretty output + JSON mode** — human-readable table formatting with colors, or `--json` for piping into other tools.
- **Version compatibility** — probes the SQLite schema at runtime to work across slackdump v3 and v4 database formats.

In short: slackdump is the engine, slackslaw is the dashboard.

## License

[MIT](LICENSE)
