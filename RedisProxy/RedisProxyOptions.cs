namespace spearedis.RedisProxy;

public sealed class RedisProxyOptions
{
    public int Port { get; set; } = 6379;

    public string PrimaryConnectionString { get; set; } = string.Empty;

    public List<string> SecondaryConnectionStrings { get; set; } = [];

    public string? AuthUsername { get; set; }

    public string? AuthPassword { get; set; }
}
