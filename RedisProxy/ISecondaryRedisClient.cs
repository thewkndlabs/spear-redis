using System.Net;
using StackExchange.Redis;

namespace spearedis.RedisProxy;

public interface ISecondaryRedisClient : IDisposable
{
    string ConnectionString { get; }

    string Endpoint { get; }

    bool IsConnected { get; }

    Task SetStringAsync(string key, string value, TimeSpan? expiry, CancellationToken cancellationToken);

    Task<string?> GetStringAsync(string key, CancellationToken cancellationToken);
}

public interface ISecondaryRedisClientFactory
{
    Task<ISecondaryRedisClient> ConnectAsync(string connectionString, CancellationToken cancellationToken);
}

public sealed class StackExchangeSecondaryRedisClientFactory : ISecondaryRedisClientFactory
{
    public async Task<ISecondaryRedisClient> ConnectAsync(string connectionString, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var multiplexer = await ConnectionMultiplexer.ConnectAsync(connectionString);
        return new StackExchangeSecondaryRedisClient(connectionString, multiplexer);
    }

    private sealed class StackExchangeSecondaryRedisClient : ISecondaryRedisClient
    {
        private readonly ConnectionMultiplexer _multiplexer;

        public StackExchangeSecondaryRedisClient(string connectionString, ConnectionMultiplexer multiplexer)
        {
            ConnectionString = connectionString;
            _multiplexer = multiplexer;
        }

        public string ConnectionString { get; }

        public string Endpoint => GetEndpointDisplay(_multiplexer, ConnectionString);

        public bool IsConnected => _multiplexer.IsConnected;

        public async Task SetStringAsync(string key, string value, TimeSpan? expiry, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var db = _multiplexer.GetDatabase();
            if (expiry.HasValue)
            {
                await db.StringSetAsync(key, value, expiry.Value);
                return;
            }

            await db.StringSetAsync(key, value);
        }

        public async Task<string?> GetStringAsync(string key, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var db = _multiplexer.GetDatabase();
            var value = await db.StringGetAsync(key);
            return value.HasValue ? value.ToString() : null;
        }

        public void Dispose()
        {
            _multiplexer.Dispose();
        }

        private static string GetEndpointDisplay(ConnectionMultiplexer multiplexer, string fallbackConnectionString)
        {
            var endpoint = multiplexer.GetEndPoints().FirstOrDefault();
            if (endpoint is null)
            {
                return SecondaryTopologyManager.TryGetEndpointFromConnectionString(fallbackConnectionString);
            }

            return endpoint switch
            {
                DnsEndPoint dns => $"{dns.Host}:{dns.Port}",
                IPEndPoint ip => $"{ip.Address}:{ip.Port}",
                _ => endpoint.ToString() ?? "unknown"
            };
        }
    }
}
