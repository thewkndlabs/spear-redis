using spearedis.RedisProxy;
using Xunit;

namespace spearedis.Tests;

public sealed class RedisProxyOptionsValidatorTests
{
    private readonly RedisProxyOptionsValidator _validator = new();

    [Fact]
    public void FailsWhenPrimaryConnectionStringIsMissing()
    {
        var options = new RedisProxyOptions
        {
            Port = 9090,
            PrimaryConnectionString = string.Empty
        };

        var result = _validator.Validate(null, options);
        var failure = result.Failures?.FirstOrDefault();

        Assert.False(result.Succeeded);
        Assert.NotNull(failure);
        Assert.Contains("PrimaryConnectionString is required", failure);
    }

    [Fact]
    public void FailsWhenSecondaryConnectionStringIsInvalid()
    {
        var options = new RedisProxyOptions
        {
            Port = 9090,
            PrimaryConnectionString = "localhost:6379",
            SecondaryConnectionStrings = [""]
        };

        var result = _validator.Validate(null, options);
        var failure = result.Failures?.FirstOrDefault();

        Assert.False(result.Succeeded);
        Assert.NotNull(failure);
        Assert.Contains("SecondaryConnectionStrings[0]", failure);
    }

    [Fact]
    public void SucceedsWithValidPrimaryAndSecondaryConnectionStrings()
    {
        var options = new RedisProxyOptions
        {
            Port = 9090,
            PrimaryConnectionString = "localhost:6379,password=secret",
            SecondaryConnectionStrings = ["localhost:6380,password=secret"]
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }
}
