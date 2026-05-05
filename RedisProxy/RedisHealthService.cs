namespace spearedis.RedisProxy;

public sealed class RedisHealthService
{
    private readonly IRedisUpstreamClient _upstreamClient;

    public RedisHealthService(IRedisUpstreamClient upstreamClient)
    {
        _upstreamClient = upstreamClient;
    }

    public HealthSummaryResponse GetLiveness()
    {
        return new HealthSummaryResponse("alive");
    }

    public ReadinessHealthResult GetReadiness()
    {
        var primary = _upstreamClient.GetPrimaryHealth();
        var isReady = primary.Connected;
        var status = isReady ? "ready" : "not-ready";

        return new ReadinessHealthResult(isReady, new HealthSummaryResponse(status));
    }

    public FullHealthResponse GetFull()
    {
        var topology = _upstreamClient.GetTopologyHealth();
        var primaryTarget = topology.FirstOrDefault(target =>
            string.Equals(target.Role, "primary", StringComparison.OrdinalIgnoreCase));

        if (primaryTarget == default)
        {
            primaryTarget = _upstreamClient.GetPrimaryHealth();
        }

        var secondaries = topology
            .Where(target => string.Equals(target.Role, "secondary", StringComparison.OrdinalIgnoreCase))
            .Select(ToResponse)
            .ToArray();

        var primaryResponse = ToResponse(primaryTarget);
        var overallStatus = !primaryTarget.Connected
            ? "unhealthy"
            : secondaries.All(s => s.Status == "healthy")
                ? "healthy"
                : "degraded";

        return new FullHealthResponse(overallStatus, primaryResponse, secondaries);
    }

    private static RedisTargetHealthResponse ToResponse(RedisTargetHealth health)
    {
        var status = health.Connected ? "healthy" : "unhealthy";
        return new RedisTargetHealthResponse(
            health.Role,
            SanitizeEndpoint(health.Endpoint),
            status);
    }

    private static string SanitizeEndpoint(string endpoint)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return "unknown";
        }

        var firstSegment = endpoint.Split(',', 2, StringSplitOptions.TrimEntries)[0];
        var credentialsSeparator = firstSegment.LastIndexOf('@');
        if (credentialsSeparator >= 0 && credentialsSeparator < firstSegment.Length - 1)
        {
            firstSegment = firstSegment[(credentialsSeparator + 1)..];
        }

        return firstSegment;
    }
}
