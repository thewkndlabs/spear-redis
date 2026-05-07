# Health Endpoints

The proxy exposes HTTP health endpoints for container and Kubernetes probes.

## Endpoints

- `GET /live`: process liveness. Returns `200` with `{ "status": "alive" }` when the service is running.
- `GET /ready`: readiness based only on primary Redis connectivity.
  - Returns `200` with `{ "status": "ready" }` when primary Redis is connected.
  - Returns `503` with `{ "status": "not-ready" }` when primary Redis is unavailable.
- `GET /full`: full topology status with redacted endpoint identity (host/IP and port only).
  - Includes primary and all secondary Redis targets.
  - Does not include usernames, passwords, or raw connection strings.

## Kubernetes Probe Guidance

Recommended probe setup:

- Liveness probe path: `/live`
- Readiness probe path: `/ready`

Using primary-only readiness prevents pod restarts or traffic eviction during secondary region incidents while still surfacing degradation through `/full`.
