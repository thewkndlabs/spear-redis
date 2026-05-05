namespace spearedis.RedisProxy;

public sealed class RedisProxyOptions
{
    public int Port { get; set; } = 6379;

    public string? AuthUsername { get; set; }

    public string? AuthPassword { get; set; }
}
