using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using spearedis.RedisProxy;
using Xunit;

namespace spearedis.Tests;

public sealed class RedisCommandProcessorTests
{
    [Fact]
    public void AuthSucceedsWithValidCredentialsAndAllowsSetGet()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            ReadResult = RedisReadResult.FoundValue("my-value")
        };

        var processor = CreateProcessor(upstream, authUsername: "user1", authPassword: "pass1");
        var clientId = Guid.NewGuid();

        var auth = processor.Handle(clientId, ["AUTH", "user1", "pass1"]);
        var set = processor.Handle(clientId, ["SET", "my-key", "my-value"]);
        var get = processor.Handle(clientId, ["GET", "my-key"]);

        Assert.Equal("+OK\r\n", auth);
        Assert.Equal("+OK\r\n", set);
        Assert.Equal("$8\r\nmy-value\r\n", get);
        Assert.Equal("my-key", upstream.LastReadKey);
        Assert.Equal("my-key", upstream.LastSetKey);
    }

    [Fact]
    public void SetReturnsErrorWhenPrimaryWriteFails()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            WriteResult = RedisWriteResult.Failed("boom")
        };

        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var set = processor.Handle(clientId, ["SET", "k", "v"]);

        Assert.Equal("-ERR primary write failed\r\n", set);
    }

    [Fact]
    public void GetReturnsErrorWhenPrimaryReadFails()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            ReadResult = RedisReadResult.Failed("boom")
        };

        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var get = processor.Handle(clientId, ["GET", "k"]);

        Assert.Equal("-ERR primary read failed\r\n", get);
    }

    [Fact]
    public void SetAndGetRequireAuthenticationWhenPasswordConfigured()
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient(), authPassword: "secret");
        var clientId = Guid.NewGuid();

        var set = processor.Handle(clientId, ["SET", "k", "v"]);
        var get = processor.Handle(clientId, ["GET", "k"]);

        Assert.Equal("-NOAUTH Authentication required.\r\n", set);
        Assert.Equal("-NOAUTH Authentication required.\r\n", get);
    }

    [Fact]
    public void SetDispatchesSecondaryReplicationWithoutBlockingClientSuccess()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            ReplicationTaskFactory = () => Task.FromException(new InvalidOperationException("secondary failed"))
        };

        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var set = processor.Handle(clientId, ["SET", "k", "v"]);

        Assert.Equal("+OK\r\n", set);
        Assert.Equal(1, upstream.ReplicationCalls);
    }

    [Fact]
    public void SetAndGetWorkWithoutAuthWhenNoPasswordConfigured()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            ReadResult = RedisReadResult.FoundValue("v")
        };

        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var set = processor.Handle(clientId, ["SET", "k", "v"]);
        var get = processor.Handle(clientId, ["GET", "k"]);

        Assert.Equal("+OK\r\n", set);
        Assert.Equal("$1\r\nv\r\n", get);
    }

    [Fact]
    public void GetReturnsNullBulkStringForMissingKey()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            ReadResult = RedisReadResult.Missing()
        };

        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var get = processor.Handle(clientId, ["GET", "missing"]);

        Assert.Equal("$-1\r\n", get);
    }

    [Theory]
    [InlineData(new[] { "AUTH" }, "-ERR wrong number of arguments for 'auth' command\r\n")]
    [InlineData(new[] { "AUTH", "a", "b", "c" }, "-ERR wrong number of arguments for 'auth' command\r\n")]
    [InlineData(new[] { "SET", "k" }, "-ERR wrong number of arguments for 'set' command\r\n")]
    [InlineData(new[] { "GET" }, "-ERR wrong number of arguments for 'get' command\r\n")]
    public void ReturnsWrongNumberOfArgumentsForInvalidArity(string[] command, string expected)
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient());
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, command);

        Assert.Equal(expected, response);
    }

    [Fact]
    public void ReturnsUnknownCommandForUnsupportedCommands()
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient());
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["MGET", "a"]);

        Assert.Equal("-ERR unknown command 'mget'\r\n", response);
    }

    [Fact]
    public void ClearsAuthenticationStateWhenClientRemoved()
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient(), authPassword: "secret");
        var clientId = Guid.NewGuid();

        var auth = processor.Handle(clientId, ["AUTH", "secret"]);
        processor.RemoveClient(clientId);
        var setAfterDisconnect = processor.Handle(clientId, ["SET", "k", "v"]);

        Assert.Equal("+OK\r\n", auth);
        Assert.Equal("-NOAUTH Authentication required.\r\n", setAfterDisconnect);
    }

    private static RedisCommandProcessor CreateProcessor(
        IRedisUpstreamClient upstream,
        string? authUsername = null,
        string? authPassword = null)
    {
        var options = Options.Create(new RedisProxyOptions
        {
            Port = 9090,
            PrimaryConnectionString = "localhost:6379",
            AuthUsername = authUsername,
            AuthPassword = authPassword
        });

        return new RedisCommandProcessor(upstream, options, NullLogger<RedisCommandProcessor>.Instance);
    }

    private sealed class FakeRedisUpstreamClient : IRedisUpstreamClient
    {
        public RedisReadResult ReadResult { get; set; } = RedisReadResult.Missing();

        public RedisWriteResult WriteResult { get; set; } = RedisWriteResult.Ok();

        public Func<Task>? ReplicationTaskFactory { get; set; }

        public string? LastReadKey { get; private set; }

        public string? LastSetKey { get; private set; }

        public int ReplicationCalls { get; private set; }

        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public RedisReadResult ReadFromPrimary(string key)
        {
            LastReadKey = key;
            return ReadResult;
        }

        public RedisWriteResult WriteToPrimary(string key, string value)
        {
            LastSetKey = key;
            return WriteResult;
        }

        public Task ReplicateToSecondariesAsync(string key, string value, CancellationToken cancellationToken = default)
        {
            ReplicationCalls++;
            LastSetKey = key;
            return ReplicationTaskFactory?.Invoke() ?? Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
