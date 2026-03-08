# slackslaw

[![Tests](https://github.com/salam/slackslaw-cli/actions/workflows/test.yml/badge.svg)](https://github.com/salam/slackslaw-cli/actions/workflows/test.yml)

A CLI for Slack that mirrors your workspaces into local SQLite for offline search, agent-friendly querying, and incremental sync. Powered by [slackdump](https://github.com/rusq/slackdump).

## Features

- **Multi-workspace** — manage and sync multiple Slack workspaces
- **Incremental sync** — only fetch new messages after the first full archive
- **Full-text search** — search across all workspaces, filter by channel, author, date range, or workspace
- **What's new / unread** — see messages since your last sync or catch up on unread, with a `--mine` filter
- **Thread view** — dump an entire thread by Slack URL
- **Channel export** — export a channel to markdown or HTML
- **Stats & status** — detailed archive statistics (top senders, busiest channels, active hours)
- **Live tail** — watch for new messages with polling (`tail -f`)
- **RAG / ask** — ask questions over your archive using Anthropic or OpenAI
- **LLM context** — dump messages as LLM-ready JSON for agent workflows
- **MCP server** — expose the archive via MCP (wraps slackdump mcp)
- **Channel keywords** — configure keywords to personalize the `--mine` filter
- **Raw SQL** — query the local SQLite databases directly
- **JSON output** — pipe results into other tools or LLM agents
- **Maintenance** — garbage collection (`gc`) and integrity checks (`repair`)
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

# View a thread by URL
slackslaw threads https://myteam.slack.com/archives/C0123ABC/p1234567890123456

# Export a channel to markdown
slackslaw export --channel "#general" > general.md

# Raw SQL
slackslaw sql 'SELECT * FROM MESSAGE LIMIT 10'

# Detailed archive statistics
slackslaw stats

# LLM-ready context dump
slackslaw context --since 7d

# Ask a question over your archive (needs ANTHROPIC_API_KEY)
slackslaw ask "what was discussed about the deployment?"
```

## Commands

| Command | Description |
|---------|-------------|
| `auth` | Add or re-authenticate a workspace |
| `workspaces` | List configured workspaces |
| `workspaces remove <name>` | Remove a workspace (`--delete-data` to purge archive) |
| `keywords` | Manage channel keywords for the `--mine` filter |
| `sync` | Incremental sync (new messages only) |
| `sync --full` | Full archive of all workspaces |
| `sync --channel "#ops"` | Sync a single channel |
| `sync --since 30d` | Time-bounded sync |
| `whatsnew` | Print messages since last sync (`--mine`, `--json`) |
| `unread` | Show unread messages (`--json`) |
| `search "expr"` | Full-text search with `--channel`, `--author`, `--after`, `--before`, `--newest-first`, `--limit` |
| `threads <url>` | Dump an entire thread by Slack URL (`--json`) |
| `stats` | Detailed statistics (top senders, busiest channels, active hours) |
| `export --channel "#name"` | Export channel to markdown or HTML (`--format html`, `--output file`) |
| `tail -f` | Live-watch for new messages (`--interval`, `--channel`) |
| `context --since 7d` | LLM-ready JSON context dump |
| `ask "question"` | RAG over the archive (`--provider openai` for OpenAI) |
| `mcp` | Start MCP server (wraps slackdump mcp) |
| `sql 'SELECT ...'` | Raw SQL against local databases |
| `status` | Archive stats per workspace |
| `gc` | Garbage collect (VACUUM databases) |
| `repair` | Check database integrity |
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
- **Thread view and export** — view threads by URL, export channels to markdown or HTML.
- **RAG / context** — dump LLM-ready JSON context or ask questions directly over the archive.
- **Pretty output + JSON mode** — human-readable table formatting with colors, or `--json` for piping into other tools.
- **Version compatibility** — probes the SQLite schema at runtime to work across slackdump v3 and v4 database formats.

In short: slackdump is the engine, slackslaw is the dashboard.

## License

[MIT](LICENSE)
