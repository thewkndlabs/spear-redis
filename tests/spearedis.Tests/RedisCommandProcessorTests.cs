using Microsoft.Extensions.Options;
using spearedis.RedisProxy;
using Xunit;

namespace spearedis.Tests;

public sealed class RedisCommandProcessorTests
{
    [Fact]
    public void AuthSucceedsWithValidCredentialsAndAllowsSetGet()
    {
        var processor = CreateProcessor(authUsername: "user1", authPassword: "pass1");
        var clientId = Guid.NewGuid();

        var auth = processor.Handle(clientId, ["AUTH", "user1", "pass1"]);
        var set = processor.Handle(clientId, ["SET", "my-key", "my-value"]);
        var get = processor.Handle(clientId, ["GET", "my-key"]);

        Assert.Equal("+OK\r\n", auth);
        Assert.Equal("+OK\r\n", set);
        Assert.Equal("$8\r\nmy-value\r\n", get);
    }

    [Fact]
    public void AuthFailsWithWrongCredentials()
    {
        var processor = CreateProcessor(authUsername: "user1", authPassword: "pass1");
        var clientId = Guid.NewGuid();

        var auth = processor.Handle(clientId, ["AUTH", "user1", "wrong"]);

        Assert.Equal("-WRONGPASS invalid username-password pair or user is disabled.\r\n", auth);
    }

    [Fact]
    public void SetAndGetRequireAuthenticationWhenPasswordConfigured()
    {
        var processor = CreateProcessor(authPassword: "secret");
        var clientId = Guid.NewGuid();

        var set = processor.Handle(clientId, ["SET", "k", "v"]);
        var get = processor.Handle(clientId, ["GET", "k"]);

        Assert.Equal("-NOAUTH Authentication required.\r\n", set);
        Assert.Equal("-NOAUTH Authentication required.\r\n", get);
    }

    [Fact]
    public void SetAndGetWorkWithoutAuthWhenNoPasswordConfigured()
    {
        var processor = CreateProcessor();
        var clientId = Guid.NewGuid();

        var set = processor.Handle(clientId, ["SET", "k", "v"]);
        var get = processor.Handle(clientId, ["GET", "k"]);

        Assert.Equal("+OK\r\n", set);
        Assert.Equal("$1\r\nv\r\n", get);
    }

    [Fact]
    public void GetReturnsNullBulkStringForMissingKey()
    {
        var processor = CreateProcessor();
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
        var processor = CreateProcessor();
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, command);

        Assert.Equal(expected, response);
    }

    [Fact]
    public void ReturnsUnknownCommandForUnsupportedCommands()
    {
        var processor = CreateProcessor();
        var clientId = Guid.NewGuid();

        var response = processor.Handle(clientId, ["MGET", "a"]);

        Assert.Equal("-ERR unknown command 'mget'\r\n", response);
    }

    [Fact]
    public void ClearsAuthenticationStateWhenClientRemoved()
    {
        var processor = CreateProcessor(authPassword: "secret");
        var clientId = Guid.NewGuid();

        var auth = processor.Handle(clientId, ["AUTH", "secret"]);
        processor.RemoveClient(clientId);
        var setAfterDisconnect = processor.Handle(clientId, ["SET", "k", "v"]);

        Assert.Equal("+OK\r\n", auth);
        Assert.Equal("-NOAUTH Authentication required.\r\n", setAfterDisconnect);
    }

    private static RedisCommandProcessor CreateProcessor(string? authUsername = null, string? authPassword = null)
    {
        var options = Options.Create(new RedisProxyOptions
        {
            Port = 6379,
            AuthUsername = authUsername,
            AuthPassword = authPassword
        });

        return new RedisCommandProcessor(new InMemoryStringKeyValueStore(), options);
    }
}
