# Done Monitor

Standalone external service that polls Linear `Done` issues, evaluates follow-up need via Claude, and optionally creates follow-up Linear tickets.

## Run

```bash
LINEAR_API_KEY=... ANTHROPIC_API_KEY=... python3 tools/done-monitor/monitor.py
```

### Run Modes

```bash
# Process all unprocessed Done issues (default)
LINEAR_API_KEY=... ANTHROPIC_API_KEY=... python3 tools/done-monitor/monitor.py --mode backfill

# Process only tickets completed after enabled_at timestamp
LINEAR_API_KEY=... ANTHROPIC_API_KEY=... python3 tools/done-monitor/monitor.py \
  --mode new_only --enabled-at 2026-03-14T21:00:00Z

# Canary run (max 1 eligible issue) with JSON summary
LINEAR_API_KEY=... ANTHROPIC_API_KEY=... python3 tools/done-monitor/monitor.py \
  --mode new_only --enabled-at 2026-03-14T21:00:00Z --max-items 1 --summary-json
```

## Runtime Files

- `processed.json` (local state)
- `monitor.lock` (concurrency guard)
- `control.json` (dashboard/API control state)

These are local runtime artifacts and are git-ignored.
