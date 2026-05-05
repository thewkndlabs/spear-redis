using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using spearedis.RedisProxy;
using Xunit;

namespace spearedis.Tests;

public sealed class RedisCommandProcessorTests
{
    [Fact]
    public void RedisCliPreflightCommandsSucceedBeforeGetSet()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            ReadResult = RedisReadResult.FoundValue("value")
        };

        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var hello = processor.Handle(clientId, ["HELLO", "3"]);
        var clientSetInfoName = processor.Handle(clientId, ["CLIENT", "SETINFO", "LIB-NAME", "redis-cli"]);
        var clientSetInfoVer = processor.Handle(clientId, ["CLIENT", "SETINFO", "LIB-VER", "8.0.0"]);
        var command = processor.Handle(clientId, ["COMMAND"]);
        var select = processor.Handle(clientId, ["SELECT", "0"]);
        var set = processor.Handle(clientId, ["SET", "preflight-key", "value"]);
        var get = processor.Handle(clientId, ["GET", "preflight-key"]);

        Assert.StartsWith("%7\r\n", hello);
        Assert.Equal("+OK\r\n", clientSetInfoName);
        Assert.Equal("+OK\r\n", clientSetInfoVer);
        Assert.Equal("*0\r\n", command);
        Assert.Equal("+OK\r\n", select);
        Assert.Equal("+OK\r\n", set);
        Assert.Equal("$5\r\nvalue\r\n", get);
    }

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
    public void SetWithExAppliesSecondsExpiry()
    {
        var upstream = new FakeRedisUpstreamClient();
        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["SET", "ttl:key", "value", "EX", "60"]);

        Assert.Equal("+OK\r\n", response);
        Assert.Equal(TimeSpan.FromSeconds(60), upstream.LastExpiry);
        Assert.Equal(TimeSpan.FromSeconds(60), upstream.LastReplicationExpiry);
    }

    [Fact]
    public void SetWithPxAppliesMillisecondsExpiry()
    {
        var upstream = new FakeRedisUpstreamClient();
        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["SET", "ttl:key", "value", "PX", "1500"]);

        Assert.Equal("+OK\r\n", response);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), upstream.LastExpiry);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), upstream.LastReplicationExpiry);
    }

    [Theory]
    [InlineData("EX", "0")]
    [InlineData("PX", "-1")]
    [InlineData("EX", "abc")]
    public void SetWithInvalidTtlValueReturnsIntegerError(string ttlOption, string ttlValue)
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient());
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["SET", "ttl:key", "value", ttlOption, ttlValue]);

        Assert.Equal("-ERR value is not an integer or out of range\r\n", response);
    }

    [Fact]
    public void SetWithUnsupportedOptionReturnsSyntaxError()
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient());
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["SET", "ttl:key", "value", "NX", "10"]);

        Assert.Equal("-ERR syntax error\r\n", response);
    }

    [Fact]
    public void KeysReturnsMatchingKeysAsRespArray()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            KeysResult = RedisKeysResult.FromKeys(["one", "two"])
        };

        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["KEYS", "*"]);

        Assert.Equal("*2\r\n$3\r\none\r\n$3\r\ntwo\r\n", response);
    }

    [Fact]
    public void KeysReturnsEmptyArrayWhenNoKeysMatch()
    {
        var upstream = new FakeRedisUpstreamClient
        {
            KeysResult = RedisKeysResult.FromKeys([])
        };

        var processor = CreateProcessor(upstream);
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["KEYS", "nomatch*"]);

        Assert.Equal("*0\r\n", response);
    }

    [Fact]
    public void KeysRequiresAuthenticationWhenConfigured()
    {
        var upstream = new FakeRedisUpstreamClient();
        var processor = CreateProcessor(upstream, authPassword: "secret");
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["KEYS", "*"]);

        Assert.Equal("-NOAUTH Authentication required.\r\n", response);
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
    [InlineData(new[] { "SET", "k", "v", "EX" }, "-ERR wrong number of arguments for 'set' command\r\n")]
    [InlineData(new[] { "SET", "k", "v", "EX", "10", "XX" }, "-ERR wrong number of arguments for 'set' command\r\n")]
    [InlineData(new[] { "GET" }, "-ERR wrong number of arguments for 'get' command\r\n")]
    [InlineData(new[] { "KEYS" }, "-ERR wrong number of arguments for 'keys' command\r\n")]
    [InlineData(new[] { "PING", "a", "b" }, "-ERR wrong number of arguments for 'ping' command\r\n")]
    [InlineData(new[] { "SELECT" }, "-ERR wrong number of arguments for 'select' command\r\n")]
    [InlineData(new[] { "QUIT", "x" }, "-ERR wrong number of arguments for 'quit' command\r\n")]
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
    public void ReturnsUnsupportedSubcommandForUnsupportedClientSubcommands()
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient());
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["CLIENT", "PAUSE"]);

        Assert.Equal("-ERR unsupported subcommand 'pause' for 'client' command\r\n", response);
    }

    [Fact]
    public void ReturnsUnsupportedSubcommandForUnsupportedCommandSubcommands()
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient());
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["COMMAND", "WAT"]);

        Assert.Equal("-ERR unsupported subcommand 'wat' for 'command' command\r\n", response);
    }

    [Fact]
    public void PingWithoutMessageReturnsPong()
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient());
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["PING"]);

        Assert.Equal("+PONG\r\n", response);
    }

    [Fact]
    public void PingWithMessageReturnsBulkString()
    {
        var processor = CreateProcessor(new FakeRedisUpstreamClient());
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["PING", "hello"]);

        Assert.Equal("$5\r\nhello\r\n", response);
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

        public RedisKeysResult KeysResult { get; set; } = RedisKeysResult.FromKeys([]);

        public RedisWriteResult WriteResult { get; set; } = RedisWriteResult.Ok();

        public Func<Task>? ReplicationTaskFactory { get; set; }

        public string? LastReadKey { get; private set; }

        public string? LastSetKey { get; private set; }

        public TimeSpan? LastExpiry { get; private set; }

        public TimeSpan? LastReplicationExpiry { get; private set; }

        public int ReplicationCalls { get; private set; }

        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public RedisReadResult ReadFromPrimary(string key)
        {
            LastReadKey = key;
            return ReadResult;
        }

        public RedisKeysResult KeysFromPrimary(string pattern)
        {
            return KeysResult;
        }

        public RedisWriteResult WriteToPrimary(string key, string value, TimeSpan? expiry = null)
        {
            LastSetKey = key;
            LastExpiry = expiry;
            return WriteResult;
        }

        public Task ReplicateToSecondariesAsync(string key, string value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        {
            ReplicationCalls++;
            LastSetKey = key;
            LastReplicationExpiry = expiry;
            return ReplicationTaskFactory?.Invoke() ?? Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
