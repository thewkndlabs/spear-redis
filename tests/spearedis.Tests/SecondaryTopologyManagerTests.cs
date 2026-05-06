using Microsoft.Extensions.Logging.Abstractions;
using spearedis.RedisProxy;
using Xunit;

namespace spearedis.Tests;

public sealed class SecondaryTopologyManagerTests
{
    [Fact]
    public async Task ReloadAddsSecondaryWithoutRestart()
    {
        var factory = new FakeSecondaryRedisClientFactory();
        using var manager = new SecondaryTopologyManager(factory, NullLogger<SecondaryTopologyManager>.Instance);

        await manager.InitializeAsync(["secondary-a:6380"], CancellationToken.None);
        await manager.ReloadAsync(["secondary-a:6380", "secondary-b:6380"], CancellationToken.None);
        await manager.ReplicateAsync("k", "v", null, CancellationToken.None);

        var secondaryA = factory.GetByConnectionString("secondary-a:6380");
        var secondaryB = factory.GetByConnectionString("secondary-b:6380");

        Assert.NotNull(secondaryA);
        Assert.NotNull(secondaryB);
        Assert.Equal(1, secondaryA!.WriteCount);
        Assert.Equal(1, secondaryB!.WriteCount);
    }

    [Fact]
    public async Task ReloadRemovesSecondaryAndDisposesConnection()
    {
        var factory = new FakeSecondaryRedisClientFactory();
        using var manager = new SecondaryTopologyManager(factory, NullLogger<SecondaryTopologyManager>.Instance);

        await manager.InitializeAsync(["secondary-a:6380", "secondary-b:6380"], CancellationToken.None);
        var removed = factory.GetByConnectionString("secondary-b:6380");

        await manager.ReloadAsync(["secondary-a:6380"], CancellationToken.None);
        await manager.ReplicateAsync("k", "v", null, CancellationToken.None);

        var remaining = factory.GetByConnectionString("secondary-a:6380");
        Assert.NotNull(removed);
        Assert.True(removed!.Disposed);
        Assert.NotNull(remaining);
        Assert.Equal(1, remaining!.WriteCount);
    }

    [Fact]
    public async Task ReloadReplacesModifiedConnectionForSameEndpoint()
    {
        var factory = new FakeSecondaryRedisClientFactory();
        using var manager = new SecondaryTopologyManager(factory, NullLogger<SecondaryTopologyManager>.Instance);

        const string oldConnection = "secondary-a:6380,password=old";
        const string newConnection = "secondary-a:6380,password=new";

        await manager.InitializeAsync([oldConnection], CancellationToken.None);
        var oldClient = factory.GetByConnectionString(oldConnection);

        await manager.ReloadAsync([newConnection], CancellationToken.None);
        await manager.ReplicateAsync("k", "v", null, CancellationToken.None);

        var newClient = factory.GetByConnectionString(newConnection);

        Assert.NotNull(oldClient);
        Assert.True(oldClient!.Disposed);
        Assert.NotNull(newClient);
        Assert.Equal(1, newClient!.WriteCount);
    }

    [Fact]
    public async Task ReloadPreservesUnaffectedEndpointsWhenOneNewEndpointFails()
    {
        var factory = new FakeSecondaryRedisClientFactory();
        factory.FailOnConnect("secondary-b:6380");

        using var manager = new SecondaryTopologyManager(factory, NullLogger<SecondaryTopologyManager>.Instance);

        await manager.InitializeAsync(["secondary-a:6380"], CancellationToken.None);

        await manager.ReloadAsync(["secondary-a:6380", "secondary-b:6380"], CancellationToken.None);
        await manager.ReplicateAsync("k", "v", null, CancellationToken.None);

        var secondaryA = factory.GetByConnectionString("secondary-a:6380");
        var secondaryB = factory.GetByConnectionString("secondary-b:6380");

        Assert.NotNull(secondaryA);
        Assert.Equal(1, secondaryA!.WriteCount);
        Assert.Null(secondaryB);
    }

    [Fact]
    public async Task ReplicationContinuesWhileReloadIsInProgress()
    {
        var factory = new FakeSecondaryRedisClientFactory();
        factory.BlockConnection("secondary-b:6380");

        using var manager = new SecondaryTopologyManager(factory, NullLogger<SecondaryTopologyManager>.Instance);

        await manager.InitializeAsync(["secondary-a:6380"], CancellationToken.None);

        var reloadTask = manager.ReloadAsync(["secondary-a:6380", "secondary-b:6380"], CancellationToken.None);

        var replicationTask = manager.ReplicateAsync("k", "v", null, CancellationToken.None);
        var completed = await Task.WhenAny(replicationTask, Task.Delay(500));

        Assert.Same(replicationTask, completed);
        await replicationTask;

        var secondaryA = factory.GetByConnectionString("secondary-a:6380");
        Assert.NotNull(secondaryA);
        Assert.Equal(1, secondaryA!.WriteCount);

        factory.UnblockConnection("secondary-b:6380");
        await reloadTask;

        await manager.ReplicateAsync("k2", "v2", null, CancellationToken.None);

        var secondaryB = factory.GetByConnectionString("secondary-b:6380");
        Assert.NotNull(secondaryB);
        Assert.Equal(1, secondaryB!.WriteCount);
    }

    private sealed class FakeSecondaryRedisClientFactory : ISecondaryRedisClientFactory
    {
        private readonly object _sync = new();
        private readonly Dictionary<string, FakeSecondaryRedisClient> _clients = new(StringComparer.Ordinal);
        private readonly HashSet<string> _failingConnections = new(StringComparer.Ordinal);
        private readonly Dictionary<string, TaskCompletionSource<bool>> _blockedConnections = new(StringComparer.Ordinal);

        public void FailOnConnect(string connectionString)
        {
            lock (_sync)
            {
                _failingConnections.Add(connectionString);
            }
        }

        public void BlockConnection(string connectionString)
        {
            lock (_sync)
            {
                _blockedConnections[connectionString] = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        public void UnblockConnection(string connectionString)
        {
            lock (_sync)
            {
                if (_blockedConnections.TryGetValue(connectionString, out var blocker))
                {
                    blocker.TrySetResult(true);
                }
            }
        }

        public FakeSecondaryRedisClient? GetByConnectionString(string connectionString)
        {
            lock (_sync)
            {
                _clients.TryGetValue(connectionString, out var client);
                return client;
            }
        }

        public async Task<ISecondaryRedisClient> ConnectAsync(string connectionString, CancellationToken cancellationToken)
        {
            Task? waitTask = null;
            lock (_sync)
            {
                if (_failingConnections.Contains(connectionString))
                {
                    throw new InvalidOperationException("connect failed");
                }

                if (_blockedConnections.TryGetValue(connectionString, out var blocker))
                {
                    waitTask = blocker.Task;
                }
            }

            if (waitTask is not null)
            {
                await waitTask.WaitAsync(cancellationToken);
            }

            var endpoint = SecondaryTopologyManager.TryGetEndpointFromConnectionString(connectionString);
            var client = new FakeSecondaryRedisClient(connectionString, endpoint);
            lock (_sync)
            {
                _clients[connectionString] = client;
            }

            return client;
        }
    }

    private sealed class FakeSecondaryRedisClient : ISecondaryRedisClient
    {
        private int _writeCount;

        public FakeSecondaryRedisClient(string connectionString, string endpoint)
        {
            ConnectionString = connectionString;
            Endpoint = endpoint;
        }

        public string ConnectionString { get; }

        public string Endpoint { get; }

        public bool IsConnected => !Disposed;

        public int WriteCount => Volatile.Read(ref _writeCount);

        public bool Disposed { get; private set; }

        public Task SetStringAsync(string key, string value, TimeSpan? expiry, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Disposed)
            {
                throw new ObjectDisposedException(nameof(FakeSecondaryRedisClient));
            }

            Interlocked.Increment(ref _writeCount);
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            Disposed = true;
        }
    }
}
