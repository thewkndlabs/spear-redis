using System.Diagnostics.Metrics;
using System.Net;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace spearedis.RedisProxy;

public sealed class RedisUpstreamClient : IRedisUpstreamClient
{
    private static readonly Meter Meter = new("spearedis.redisproxy");
    private static readonly Counter<long> SecondaryReplicationSuccess = Meter.CreateCounter<long>("redisproxy.secondary.replication.success");
    private static readonly Counter<long> SecondaryReplicationFailure = Meter.CreateCounter<long>("redisproxy.secondary.replication.failure");

    private RedisProxyOptions _options;
    private readonly IOptionsMonitor<RedisProxyOptions> _optionsMonitor;
    private readonly SecondaryTopologyManager _secondaryTopology;
    private readonly ILogger<RedisUpstreamClient> _logger;
    private readonly IDisposable? _optionsReloadSubscription;

    private readonly object _sync = new();
    private bool _initialized;
    private ConnectionMultiplexer? _primary;
    private bool _disposed;

    public RedisUpstreamClient(
        IOptionsMonitor<RedisProxyOptions> optionsMonitor,
        SecondaryTopologyManager secondaryTopology,
        ILogger<RedisUpstreamClient> logger)
    {
        _optionsMonitor = optionsMonitor;
        _options = optionsMonitor.CurrentValue;
        _secondaryTopology = secondaryTopology;
        _logger = logger;
        _optionsReloadSubscription = optionsMonitor.OnChange(OnOptionsChanged);
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
        await _secondaryTopology.InitializeAsync(_options.SecondaryConnectionStrings, cancellationToken);

        lock (_sync)
        {
            if (_initialized)
            {
                primary.Dispose();

                return;
            }

            _primary = primary;
            _initialized = true;
        }

        _logger.LogInformation(
            "Initialized Redis upstream topology with primary and {SecondaryCount} secondaries",
            _secondaryTopology.Count);
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

    public RedisTargetHealth GetPrimaryHealth()
    {
        var primary = _primary;
        var endpoint = primary is null
            ? TryGetEndpointFromConnectionString(_options.PrimaryConnectionString)
            : GetEndpointDisplay(primary);

        return new RedisTargetHealth("primary", endpoint, _initialized && primary is not null && primary.IsConnected);
    }

    public IReadOnlyList<RedisTargetHealth> GetTopologyHealth()
    {
        var targets = new List<RedisTargetHealth>(1 + _secondaryTopology.Count)
        {
            GetPrimaryHealth()
        };

        targets.AddRange(_secondaryTopology.GetHealth());

        return targets;
    }

    public RedisKeysResult KeysFromPrimary(string pattern)
    {
        var primary = _primary;
        if (!_initialized || primary is null)
        {
            return RedisKeysResult.Failed("Redis upstream is not initialized.");
        }

        try
        {
            var endpoints = primary.GetEndPoints();
            if (endpoints.Length == 0)
            {
                return RedisKeysResult.Failed("Primary Redis endpoint not available.");
            }

            var server = primary.GetServer(endpoints[0]);
            var keys = server.Keys(pattern: pattern)
                .Select(k => k.ToString())
                .ToArray();

            return RedisKeysResult.FromKeys(keys);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Primary Redis KEYS failed for pattern {Pattern}", pattern);
            return RedisKeysResult.Failed("Primary Redis KEYS failed.");
        }
    }

    public RedisWriteResult WriteToPrimary(string key, string value, TimeSpan? expiry = null)
    {
        var primary = _primary;
        if (!_initialized || primary is null)
        {
            return RedisWriteResult.Failed("Redis upstream is not initialized.");
        }

        try
        {
            var db = primary.GetDatabase();
            var setResult = expiry.HasValue
                ? db.StringSet(key, value, expiry.Value)
                : db.StringSet(key, value);
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

    public async Task ReplicateToSecondariesAsync(string key, string value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
    {
        if (!_initialized || _secondaryTopology.Count == 0)
        {
            return;
        }

        var targets = _secondaryTopology.GetReplicationTargets();
        var tasks = targets.Select((target, index) => ReplicateToSecondaryAsync(target, index, key, value, expiry, cancellationToken));
        await Task.WhenAll(tasks);
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            _primary?.Dispose();
            _primary = null;
            _initialized = false;
        }

        _optionsReloadSubscription?.Dispose();
        _secondaryTopology.Dispose();
    }

    private async void OnOptionsChanged(RedisProxyOptions? options)
    {
        if (options is null)
        {
            return;
        }

        _options = options;

        if (!_initialized || _disposed)
        {
            return;
        }

        try
        {
            await _secondaryTopology.ReloadAsync(options.SecondaryConnectionStrings, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Secondary topology reload failed.");
        }
    }

    private async Task ReplicateToSecondaryAsync(ISecondaryRedisClient secondary, int index, string key, string value, TimeSpan? expiry, CancellationToken cancellationToken)
    {
        try
        {
            await secondary.SetStringAsync(key, value, expiry, cancellationToken);
            SecondaryReplicationSuccess.Add(1);
            _logger.LogInformation("Secondary replication succeeded for key {Key} on target {TargetIndex}", key, index);
        }
        catch (Exception ex)
        {
            SecondaryReplicationFailure.Add(1);
            _logger.LogWarning(ex, "Secondary replication failed for key {Key} on target {TargetIndex}", key, index);
        }
    }

    private static string GetEndpointDisplay(ConnectionMultiplexer multiplexer)
    {
        var endpoint = multiplexer.GetEndPoints().FirstOrDefault();
        if (endpoint is null)
        {
            return "unknown";
        }

        return FormatEndpoint(endpoint);
    }

    private static string TryGetEndpointFromConnectionString(string connectionString)
    {
        try
        {
            var options = ConfigurationOptions.Parse(connectionString);
            var endpoint = options.EndPoints.FirstOrDefault();
            if (endpoint is null)
            {
                return "unknown";
            }

            return FormatEndpoint(endpoint);
        }
        catch
        {
            var raw = connectionString.Split(',', 2, StringSplitOptions.TrimEntries)[0];
            return string.IsNullOrWhiteSpace(raw) ? "unknown" : raw;
        }
    }

    private static string FormatEndpoint(EndPoint endpoint)
    {
        return endpoint switch
        {
            DnsEndPoint dns => $"{dns.Host}:{dns.Port}",
            IPEndPoint ip => $"{ip.Address}:{ip.Port}",
            _ => endpoint.ToString() ?? "unknown"
        };
    }
}
