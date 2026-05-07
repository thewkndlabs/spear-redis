# spearedis

A lightweight Redis RESP fan-out proxy built with .NET.

- Clients connect to this proxy using RESP.
- `GET` reads from the configured primary Redis.
- `SET` writes to primary and replicates asynchronously to configured secondaries.
- Health endpoints are exposed for liveness/readiness/topology checks.

## Prerequisites

- .NET SDK 10+
- Redis instances reachable by configured connection strings
- Docker (optional, for containerized local run)

## Basic Configuration

Default configuration is in appsettings:

- `RedisProxy:Port` (default `9379`): RESP listener port
- `RedisProxy:PrimaryConnectionString` (default `localhost:6379`)
- `RedisProxy:SecondaryConnectionStrings` (default `localhost:6380`, `6381`, `6382`)
- `RedisProxy:AuthUsername` (optional)
- `RedisProxy:AuthPassword` (optional)

Supported environment-variable overrides:

- `REDIS_PROXY_PORT`
- `REDIS_PROXY_PRIMARY_CONNECTION_STRING`
- `REDIS_PROXY_SECONDARY_CONNECTION_STRINGS` (multiple values separated by `|`)
- `REDIS_PROXY_SECONDARY_CONNECTION_STRING` (single secondary convenience override)

## Run Path 1: dotnet run

1. Start at least one primary and one secondary Redis locally.

```bash
docker run -d --rm --name spearedis-primary -p 6379:6379 redis:7.4-alpine
docker run -d --rm --name spearedis-secondary-1 -p 6380:6379 redis:7.4-alpine
```

2. Run the proxy:

```bash
REDIS_PROXY_SECONDARY_CONNECTION_STRINGS=localhost:6380 dotnet run
```

By default, HTTP is available at `http://localhost:9090` (launch profile), and RESP listens on `localhost:9379`.

## Run Path 2: docker compose

Start proxy + Redis topology:

```bash
docker compose up -d --build
```

With compose defaults:

- HTTP: `http://localhost:18080`
- RESP: `localhost:19379`

Stop:

```bash
docker compose down
```

## Verify Startup

HTTP health checks:

```bash
curl -f http://localhost:9090/health/live
curl -f http://localhost:9090/health/ready
```

For compose flow, replace `9090` with `18080`.

RESP reachability check:

```bash
redis-cli -p 9379 ping
```

For compose flow, use `-p 19379`.

## More Docs

- Containerized local deployment: [docs/containerized-local-deployment.md](docs/containerized-local-deployment.md)
- Health endpoints: [docs/health-endpoints.md](docs/health-endpoints.md)
- Redis CLI compatibility notes: [docs/redis-cli-compatibility.md](docs/redis-cli-compatibility.md)
- Runtime topology reload behavior: [docs/runtime-topology-reload.md](docs/runtime-topology-reload.md)
- Stress test script usage: [docs/redis-stress-test.md](docs/redis-stress-test.md)
