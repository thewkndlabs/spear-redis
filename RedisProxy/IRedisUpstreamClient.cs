namespace spearedis.RedisProxy;

public interface IRedisUpstreamClient : IDisposable
{
    Task InitializeAsync(CancellationToken cancellationToken);

    RedisReadResult ReadFromPrimary(string key);

    RedisKeysResult KeysFromPrimary(string pattern);

    RedisWriteResult WriteToPrimary(string key, string value, TimeSpan? expiry = null);

    Task ReplicateToSecondariesAsync(string key, string value, TimeSpan? expiry = null, CancellationToken cancellationToken = default);
}
