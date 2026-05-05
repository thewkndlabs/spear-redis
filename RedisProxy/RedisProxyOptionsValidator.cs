using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace spearedis.RedisProxy;

public sealed class RedisProxyOptionsValidator : IValidateOptions<RedisProxyOptions>
{
    public ValidateOptionsResult Validate(string? name, RedisProxyOptions options)
    {
        if (options.Port is < 1 or > 65535)
        {
            return ValidateOptionsResult.Fail("RedisProxy:Port must be between 1 and 65535.");
        }

        var hasUsername = !string.IsNullOrWhiteSpace(options.AuthUsername);
        var hasPassword = !string.IsNullOrWhiteSpace(options.AuthPassword);
        if (hasUsername && !hasPassword)
        {
            return ValidateOptionsResult.Fail("RedisProxy auth configuration is invalid: AuthUsername requires AuthPassword.");
        }

        if (string.IsNullOrWhiteSpace(options.PrimaryConnectionString))
        {
            return ValidateOptionsResult.Fail("RedisProxy:PrimaryConnectionString is required.");
        }

        if (!IsValidConnectionString(options.PrimaryConnectionString))
        {
            return ValidateOptionsResult.Fail("RedisProxy:PrimaryConnectionString is not a valid Redis connection string.");
        }

        for (var i = 0; i < options.SecondaryConnectionStrings.Count; i++)
        {
            var secondary = options.SecondaryConnectionStrings[i];
            if (!IsValidConnectionString(secondary))
            {
                return ValidateOptionsResult.Fail($"RedisProxy:SecondaryConnectionStrings[{i}] is not a valid Redis connection string.");
            }
        }

        return ValidateOptionsResult.Success;
    }

    private static bool IsValidConnectionString(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return false;
        }

        try
        {
            ConfigurationOptions.Parse(connectionString);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
