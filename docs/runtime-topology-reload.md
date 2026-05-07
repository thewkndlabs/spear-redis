# Runtime Redis Topology Reload

The proxy supports runtime reconfiguration of secondary Redis targets from configuration reloads (for example, appsettings updates from Kubernetes ConfigMap mounts).

## Behavior

- Primary Redis remains the source of truth for GET and synchronous SET writes.
- Secondary targets are reconciled at runtime when configuration changes.
- Add: new secondary endpoints are connected and then included in replication fan-out.
- Remove: removed secondary endpoints stop receiving writes and are disposed.
- Modify: changed connection details for the same endpoint replace the old connection.
- Failed secondary updates do not stop command processing; unaffected active secondaries remain in service.

## Availability Semantics

- Redis command handling continues while secondary topology reload is happening.
- Readiness remains based on primary Redis availability only.
- Secondary reload failures are logged with endpoint context.

## Configuration

The runtime reload is driven by updates to the `RedisProxy:SecondaryConnectionStrings` configuration value source.

Examples:

```json
{
  "RedisProxy": {
    "SecondaryConnectionStrings": [
      "redis-secondary-1:6379",
      "redis-secondary-2:6379,password=secret"
    ]
  }
}
```

Environment override options are still supported for startup configuration, but ConfigMap-backed appsettings updates are the recommended path for runtime topology changes.
