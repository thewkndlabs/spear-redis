# Redis Stress Test Script

This repo now includes a Bun-based Redis stress script at `scripts/redis-stress.ts`.
It uses the `redis` package and relies on Bun to auto-resolve/install dependencies when you run it, so there is no committed `node_modules/` directory.

## Run

```bash
bun run stress:redis -- --connection-string redis://127.0.0.1:9379
```

## Common Examples

Write-heavy test against the proxy:

```bash
bun run stress:redis -- --connection-string redis://127.0.0.1:9379 --mode write --clients 16 --pipeline 32 --duration 60
```

Mixed reads and writes against a direct Redis node:

```bash
bun run stress:redis -- --connection-string redis://127.0.0.1:6379 --mode mixed --read-percent 70 --clients 24 --pipeline 32
```

Authenticated target:

```bash
bun run stress:redis -- --connection-string redis://default:secret@127.0.0.1:6379
```

## Configuration

CLI flags take precedence over environment variables.

- `--connection-string` or `REDIS_URL`
- `--mode` or `REDIS_STRESS_MODE`
- `--clients` or `REDIS_STRESS_CLIENTS`
- `--pipeline` or `REDIS_STRESS_PIPELINE`
- `--duration` or `REDIS_STRESS_DURATION_SEC`
- `--max-ops` or `REDIS_STRESS_MAX_OPS`
- `--read-percent` or `REDIS_STRESS_READ_PERCENT`
- `--key-prefix` or `REDIS_STRESS_KEY_PREFIX`
- `--keyspace` or `REDIS_STRESS_KEYSPACE`
- `--value-size` or `REDIS_STRESS_VALUE_SIZE`
- `--report-ms` or `REDIS_STRESS_REPORT_MS`

The script emits JSON progress lines with throughput and latency summaries so it can be piped into other tooling if needed.
