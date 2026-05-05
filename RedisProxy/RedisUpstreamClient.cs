using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace spearedis.RedisProxy;

public sealed class RedisUpstreamClient : IRedisUpstreamClient
{
    private static readonly Meter Meter = new("spearedis.redisproxy");
    private static readonly Counter<long> SecondaryReplicationSuccess = Meter.CreateCounter<long>("redisproxy.secondary.replication.success");
    private static readonly Counter<long> SecondaryReplicationFailure = Meter.CreateCounter<long>("redisproxy.secondary.replication.failure");

    private readonly RedisProxyOptions _options;
    private readonly ILogger<RedisUpstreamClient> _logger;

    private readonly object _sync = new();
    private bool _initialized;
    private ConnectionMultiplexer? _primary;
    private readonly List<ConnectionMultiplexer> _secondaries = [];

    public RedisUpstreamClient(IOptions<RedisProxyOptions> options, ILogger<RedisUpstreamClient> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (_initialized)
            {
                return;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();

        var primary = await ConnectionMultiplexer.ConnectAsync(_options.PrimaryConnectionString);

        var secondaries = new List<ConnectionMultiplexer>(_options.SecondaryConnectionStrings.Count);
        foreach (var secondaryConnectionString in _options.SecondaryConnectionStrings)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var secondary = await ConnectionMultiplexer.ConnectAsync(secondaryConnectionString);
            secondaries.Add(secondary);
        }

        lock (_sync)
        {
            if (_initialized)
            {
                primary.Dispose();
                foreach (var secondary in secondaries)
                {
                    secondary.Dispose();
                }

                return;
            }

            _primary = primary;
            _secondaries.AddRange(secondaries);
            _initialized = true;
        }

        _logger.LogInformation(
            "Initialized Redis upstream topology with primary and {SecondaryCount} secondaries",
            _secondaries.Count);
    }

    public RedisReadResult ReadFromPrimary(string key)
    {
        var primary = _primary;
        if (!_initialized || primary is null)
        {
            return RedisReadResult.Failed("Redis upstream is not initialized.");
        }

        try
        {
            var value = primary.GetDatabase().StringGet(key);
            if (!value.HasValue)
            {
                return RedisReadResult.Missing();
            }

            return RedisReadResult.FoundValue(value.ToString());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Primary Redis read failed for key {Key}", key);
            return RedisReadResult.Failed("Primary Redis read failed.");
        }
    }

    public RedisWriteResult WriteToPrimary(string key, string value)
    {
        var primary = _primary;
        if (!_initialized || primary is null)
        {
            return RedisWriteResult.Failed("Redis upstream is not initialized.");
        }

        try
        {
            var setResult = primary.GetDatabase().StringSet(key, value);
            if (!setResult)
            {
                return RedisWriteResult.Failed("Primary Redis write failed.");
            }

            return RedisWriteResult.Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Primary Redis write failed for key {Key}", key);
            return RedisWriteResult.Failed("Primary Redis write failed.");
        }
    }

    public async Task ReplicateToSecondariesAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        if (!_initialized || _secondaries.Count == 0)
        {
            return;
        }

        var tasks = _secondaries.Select((secondary, index) => ReplicateToSecondaryAsync(secondary, index, key, value, cancellationToken));
        await Task.WhenAll(tasks);
    }

    public void Dispose()
    {
        lock (_sync)
        {
            _primary?.Dispose();
            _primary = null;

            foreach (var secondary in _secondaries)
            {
                secondary.Dispose();
            }

            _secondaries.Clear();
            _initialized = false;
        }
    }

    private async Task ReplicateToSecondaryAsync(ConnectionMultiplexer secondary, int index, string key, string value, CancellationToken cancellationToken)
    {
        try
        {
            await secondary.GetDatabase().StringSetAsync(key, value);
            SecondaryReplicationSuccess.Add(1);
            _logger.LogInformation("Secondary replication succeeded for key {Key} on target {TargetIndex}", key, index);
        }
        catch (Exception ex)
        {
            SecondaryReplicationFailure.Add(1);
            _logger.LogWarning(ex, "Secondary replication failed for key {Key} on target {TargetIndex}", key, index);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }
}
