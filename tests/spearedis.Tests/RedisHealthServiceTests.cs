using spearedis.RedisProxy;
using Xunit;

namespace spearedis.Tests;

public sealed class RedisHealthServiceTests
{
    [Fact]
    public void LiveReturnsAlive()
    {
        var service = new RedisHealthService(new FakeRedisUpstreamClient());

        var response = service.GetLiveness();

        Assert.Equal("alive", response.Status);
    }

    [Fact]
    public void ReadyReturnsReadyWhenPrimaryIsConnected()
    {
        var service = new RedisHealthService(new FakeRedisUpstreamClient
        {
            PrimaryHealth = new RedisTargetHealth("primary", "10.1.0.1:6379", true)
        });

        var readiness = service.GetReadiness();

        Assert.True(readiness.IsReady);
        Assert.Equal("ready", readiness.Response.Status);
    }

    [Fact]
    public void ReadyReturnsNotReadyWhenPrimaryIsDisconnected()
    {
        var service = new RedisHealthService(new FakeRedisUpstreamClient
        {
            PrimaryHealth = new RedisTargetHealth("primary", "10.1.0.1:6379", false)
        });

        var readiness = service.GetReadiness();

        Assert.False(readiness.IsReady);
        Assert.Equal("not-ready", readiness.Response.Status);
    }

    [Fact]
    public void ReadyIgnoresSecondaryFailuresWhenPrimaryIsConnected()
    {
        var service = new RedisHealthService(new FakeRedisUpstreamClient
        {
            PrimaryHealth = new RedisTargetHealth("primary", "10.1.0.1:6379", true),
            TopologyHealth =
            [
                new RedisTargetHealth("primary", "10.1.0.1:6379", true),
                new RedisTargetHealth("secondary", "10.2.0.1:6379", false),
                new RedisTargetHealth("secondary", "10.3.0.1:6379", false)
            ]
        });

        var readiness = service.GetReadiness();

        Assert.True(readiness.IsReady);
        Assert.Equal("ready", readiness.Response.Status);
    }

    [Fact]
    public void FullReturnsTopologyAndRedactsCredentials()
    {
        var service = new RedisHealthService(new FakeRedisUpstreamClient
        {
            PrimaryHealth = new RedisTargetHealth("primary", "user:secret@10.1.0.1:6379,password=hidden", true),
            TopologyHealth =
            [
                new RedisTargetHealth("primary", "user:secret@10.1.0.1:6379,password=hidden", true),
                new RedisTargetHealth("secondary", "secondary.local:6380,password=hidden", false)
            ]
        });

        var full = service.GetFull();

        Assert.Equal("degraded", full.Status);
        Assert.Equal("10.1.0.1:6379", full.Primary.Endpoint);
        Assert.Equal("healthy", full.Primary.Status);
        Assert.Single(full.Secondaries);
        Assert.Equal("secondary.local:6380", full.Secondaries[0].Endpoint);
        Assert.Equal("unhealthy", full.Secondaries[0].Status);
    }

    private sealed class FakeRedisUpstreamClient : IRedisUpstreamClient
    {
        public RedisTargetHealth PrimaryHealth { get; set; } = new("primary", "localhost:6379", true);

        public IReadOnlyList<RedisTargetHealth> TopologyHealth { get; set; } =
        [
            new RedisTargetHealth("primary", "localhost:6379", true)
        ];

        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public RedisTargetHealth GetPrimaryHealth() => PrimaryHealth;

        public IReadOnlyList<RedisTargetHealth> GetTopologyHealth() => TopologyHealth;

        public RedisReadResult ReadFromPrimary(string key) => RedisReadResult.Missing();

        public RedisKeysResult KeysFromPrimary(string pattern) => RedisKeysResult.FromKeys([]);

        public RedisWriteResult WriteToPrimary(string key, string value, TimeSpan? expiry = null) => RedisWriteResult.Ok();

        public Task ReplicateToSecondariesAsync(string key, string value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public void Dispose()
        {
        }
    }
}
