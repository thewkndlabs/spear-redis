using StackExchange.Redis;

namespace spearedis.RedisProxy;

public sealed class SecondaryTopologyManager : IDisposable
{
    private readonly ISecondaryRedisClientFactory _factory;
    private readonly ILogger<SecondaryTopologyManager> _logger;
    private readonly SemaphoreSlim _reloadGate = new(1, 1);

    private SecondaryEntry[] _entries = [];

    public SecondaryTopologyManager(ISecondaryRedisClientFactory factory, ILogger<SecondaryTopologyManager> logger)
    {
        _factory = factory;
        _logger = logger;
    }

    public int Count => Volatile.Read(ref _entries).Length;

    public IReadOnlyList<RedisTargetHealth> GetHealth()
    {
        var snapshot = Volatile.Read(ref _entries);
        return snapshot
            .Select(entry => new RedisTargetHealth("secondary", entry.Client.Endpoint, entry.Client.IsConnected))
            .ToArray();
    }

    public IReadOnlyList<ISecondaryRedisClient> GetReplicationTargets()
    {
        var snapshot = Volatile.Read(ref _entries);
        return snapshot.Select(entry => entry.Client).ToArray();
    }

    public async Task InitializeAsync(IReadOnlyList<string> connectionStrings, CancellationToken cancellationToken)
    {
        var newEntries = new List<SecondaryEntry>(connectionStrings.Count);
        try
        {
            foreach (var connectionString in connectionStrings)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var endpointKey = GetEndpointKey(connectionString);
                var client = await _factory.ConnectAsync(connectionString, cancellationToken);
                newEntries.Add(new SecondaryEntry(endpointKey, connectionString, client));
            }
        }
        catch
        {
            foreach (var entry in newEntries)
            {
                entry.Client.Dispose();
            }

            throw;
        }

        var oldEntries = Interlocked.Exchange(ref _entries, newEntries.ToArray());
        DisposeEntries(oldEntries);
    }

    public async Task ReloadAsync(IReadOnlyList<string> desiredConnectionStrings, CancellationToken cancellationToken)
    {
        await _reloadGate.WaitAsync(cancellationToken);
        try
        {
            var current = Volatile.Read(ref _entries);
            var currentByKey = current.ToDictionary(entry => entry.EndpointKey, StringComparer.Ordinal);
            var desiredByKey = new Dictionary<string, string>(StringComparer.Ordinal);
            var desiredOrder = new List<string>();

            foreach (var connectionString in desiredConnectionStrings)
            {
                if (!TryGetEndpointKey(connectionString, out var endpointKey))
                {
                    _logger.LogWarning(
                        "Secondary topology reload skipped invalid endpoint connection string: {ConnectionString}",
                        connectionString);
                    continue;
                }

                if (desiredByKey.TryGetValue(endpointKey, out var previous))
                {
                    _logger.LogWarning(
                        "Secondary topology reload ignored duplicate endpoint {Endpoint}. Keeping first connection string and dropping duplicate.",
                        endpointKey);
                    continue;
                }

                desiredByKey[endpointKey] = connectionString;
                desiredOrder.Add(endpointKey);
            }

            var nextEntries = new List<SecondaryEntry>(desiredOrder.Count);
            var toDispose = new List<ISecondaryRedisClient>();

            foreach (var endpointKey in desiredOrder)
            {
                var desiredConnection = desiredByKey[endpointKey];

                if (currentByKey.TryGetValue(endpointKey, out var currentEntry))
                {
                    if (string.Equals(currentEntry.ConnectionString, desiredConnection, StringComparison.Ordinal))
                    {
                        nextEntries.Add(currentEntry);
                        continue;
                    }

                    var replacement = await TryConnectAsync(desiredConnection, endpointKey, cancellationToken);
                    if (replacement is null)
                    {
                        nextEntries.Add(currentEntry);
                        continue;
                    }

                    nextEntries.Add(new SecondaryEntry(endpointKey, desiredConnection, replacement));
                    toDispose.Add(currentEntry.Client);
                    _logger.LogInformation("Secondary topology reload replaced endpoint {Endpoint}", endpointKey);
                    continue;
                }

                var added = await TryConnectAsync(desiredConnection, endpointKey, cancellationToken);
                if (added is null)
                {
                    continue;
                }

                nextEntries.Add(new SecondaryEntry(endpointKey, desiredConnection, added));
                _logger.LogInformation("Secondary topology reload added endpoint {Endpoint}", endpointKey);
            }

            var nextKeys = nextEntries.Select(entry => entry.EndpointKey).ToHashSet(StringComparer.Ordinal);
            foreach (var currentEntry in current)
            {
                if (nextKeys.Contains(currentEntry.EndpointKey))
                {
                    continue;
                }

                toDispose.Add(currentEntry.Client);
                _logger.LogInformation("Secondary topology reload removed endpoint {Endpoint}", currentEntry.EndpointKey);
            }

            Interlocked.Exchange(ref _entries, nextEntries.ToArray());

            DisposeEntries(toDispose);

            _logger.LogInformation(
                "Secondary topology reload applied. Desired={DesiredCount}, Active={ActiveCount}",
                desiredByKey.Count,
                nextEntries.Count);
        }
        finally
        {
            _reloadGate.Release();
        }
    }

    public async Task ReplicateAsync(string key, string value, TimeSpan? expiry, CancellationToken cancellationToken)
    {
        var snapshot = Volatile.Read(ref _entries);
        if (snapshot.Length == 0)
        {
            return;
        }

        var tasks = snapshot.Select(entry => entry.Client.SetStringAsync(key, value, expiry, cancellationToken));
        await Task.WhenAll(tasks);
    }

    public void Dispose()
    {
        _reloadGate.Dispose();
        var previous = Interlocked.Exchange(ref _entries, []);
        DisposeEntries(previous);
    }

    public static string TryGetEndpointFromConnectionString(string connectionString)
    {
        try
        {
            var options = ConfigurationOptions.Parse(connectionString);
            var endpoint = options.EndPoints.FirstOrDefault();
            return endpoint?.ToString() ?? "unknown";
        }
        catch
        {
            var raw = connectionString.Split(',', 2, StringSplitOptions.TrimEntries)[0];
            return string.IsNullOrWhiteSpace(raw) ? "unknown" : raw;
        }
    }

    private static void DisposeEntries(IEnumerable<SecondaryEntry> entries)
    {
        foreach (var entry in entries)
        {
            entry.Client.Dispose();
        }
    }

    private static void DisposeEntries(IEnumerable<ISecondaryRedisClient> clients)
    {
        foreach (var client in clients)
        {
            client.Dispose();
        }
    }

    private async Task<ISecondaryRedisClient?> TryConnectAsync(string connectionString, string endpointKey, CancellationToken cancellationToken)
    {
        try
        {
            return await _factory.ConnectAsync(connectionString, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Secondary topology reload failed for endpoint {Endpoint}", endpointKey);
            return null;
        }
    }

    private static string GetEndpointKey(string connectionString)
    {
        if (!TryGetEndpointKey(connectionString, out var endpointKey))
        {
            throw new InvalidOperationException($"Secondary endpoint is invalid: {connectionString}");
        }

        return endpointKey;
    }

    private static bool TryGetEndpointKey(string connectionString, out string endpointKey)
    {
        endpointKey = string.Empty;
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return false;
        }

        try
        {
            var options = ConfigurationOptions.Parse(connectionString);
            var endpoint = options.EndPoints.FirstOrDefault();
            if (endpoint is null)
            {
                return false;
            }

            endpointKey = endpoint.ToString() ?? string.Empty;
            return !string.IsNullOrWhiteSpace(endpointKey);
        }
        catch
        {
            return false;
        }
    }

    private sealed record SecondaryEntry(string EndpointKey, string ConnectionString, ISecondaryRedisClient Client);
}
