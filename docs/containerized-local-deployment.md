# Containerized Local Deployment

This project includes Docker assets to run the proxy with a local Redis topology.

## Prerequisites

- Docker Engine with Compose plugin available (`docker compose`)

## Build Image

Build the proxy image from repository root:

```bash
docker build -t spearedis:local .
```

## Run Full Local Stack

Start proxy + primary Redis + secondary Redis:

```bash
docker compose up -d --build
```

Check containers:

```bash
docker compose ps
```

Check proxy readiness endpoint:

```bash
curl -f http://localhost:18080/health/ready
```

Proxy RESP endpoint is published on `localhost:19379`.

## Stop Stack

```bash
docker compose down
```

To also remove Redis data volumes (if added later), use:

```bash
docker compose down -v
```

## Compose Runtime Configuration

The proxy service receives container-friendly environment overrides:

- `REDIS_PROXY_PORT=9379`
- `REDIS_PROXY_PRIMARY_CONNECTION_STRING=redis-primary:6379`
- `REDIS_PROXY_SECONDARY_CONNECTION_STRINGS=redis-secondary-1:6379`

These map directly to the existing startup override behavior in the application.
