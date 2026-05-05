namespace spearedis.RedisProxy;

public sealed record HealthSummaryResponse(string Status);

public sealed record RedisTargetHealthResponse(string Role, string Endpoint, string Status);

public sealed record FullHealthResponse(
    string Status,
    RedisTargetHealthResponse Primary,
    IReadOnlyList<RedisTargetHealthResponse> Secondaries);

public readonly record struct ReadinessHealthResult(bool IsReady, HealthSummaryResponse Response);
