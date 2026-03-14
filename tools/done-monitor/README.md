# Done Monitor

Standalone external service that polls Linear `Done` issues, evaluates follow-up need via Claude, and optionally creates follow-up Linear tickets.

## Run

```bash
LINEAR_API_KEY=... ANTHROPIC_API_KEY=... python3 tools/done-monitor/monitor.py
```

## Runtime Files

- `processed.json` (local state)
- `monitor.lock` (concurrency guard)

These are local runtime artifacts and are git-ignored.
