namespace spearedis.RedisProxy;

public interface IRedisUpstreamClient : IDisposable
{
    Task InitializeAsync(CancellationToken cancellationToken);

    RedisReadResult ReadFromPrimary(string key);

    RedisWriteResult WriteToPrimary(string key, string value);

    Task ReplicateToSecondariesAsync(string key, string value, CancellationToken cancellationToken = default);
}
