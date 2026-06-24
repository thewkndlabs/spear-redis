namespace spearedis.RedisProxy;

public sealed class RedisProxyOptions
{
    public int Port { get; set; } = 6379;

    public string PrimaryConnectionString { get; set; } = string.Empty;

    public List<string> SecondaryConnectionStrings { get; set; } = [];

    public string ReplayQueueSqlitePath { get; set; } = "data/replay-queue.db";

    public int ReplayBatchSize { get; set; } = 100;

    public int ReplayIntervalMilliseconds { get; set; } = 1000;

    public int ReplayRetryBackoffMilliseconds { get; set; } = 5000;

    public string? AuthUsername { get; set; }

    public string? AuthPassword { get; set; }
}
