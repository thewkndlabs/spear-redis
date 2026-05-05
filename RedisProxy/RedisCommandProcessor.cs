using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace spearedis.RedisProxy;

public sealed class RedisCommandProcessor
{
    private const string Ok = "+OK\r\n";
    private const string NullBulkString = "$-1\r\n";
    private const string NoAuth = "-NOAUTH Authentication required.\r\n";
    private const string WrongPass = "-WRONGPASS invalid username-password pair or user is disabled.\r\n";

    private readonly IStringKeyValueStore _store;
    private readonly RedisProxyOptions _options;
    private readonly ConcurrentDictionary<Guid, bool> _authenticatedClients = new();

    public RedisCommandProcessor(IStringKeyValueStore store, IOptions<RedisProxyOptions> options)
    {
        _store = store;
        _options = options.Value;
    }

    public string Handle(Guid clientId, IReadOnlyList<string> arguments)
    {
        if (arguments.Count == 0)
        {
            return "-ERR unknown command ''\r\n";
        }

        var command = arguments[0].ToUpperInvariant();

        return command switch
        {
            "AUTH" => HandleAuth(clientId, arguments),
            "SET" => HandleSet(clientId, arguments),
            "GET" => HandleGet(clientId, arguments),
            _ => $"-ERR unknown command '{command.ToLowerInvariant()}'\r\n"
        };
    }

    public void RemoveClient(Guid clientId)
    {
        _authenticatedClients.TryRemove(clientId, out _);
    }

    private string HandleAuth(Guid clientId, IReadOnlyList<string> args)
    {
        if (args.Count is not (2 or 3))
        {
            return WrongArguments("auth");
        }

        if (!RequiresAuthentication())
        {
            _authenticatedClients[clientId] = true;
            return Ok;
        }

        string? username = null;
        string password;

        if (args.Count == 2)
        {
            password = args[1];
        }
        else
        {
            username = args[1];
            password = args[2];
        }

        var expectedPassword = _options.AuthPassword ?? string.Empty;
        var expectedUsername = _options.AuthUsername;

        var usernameMatches = string.IsNullOrEmpty(expectedUsername) || string.Equals(expectedUsername, username, StringComparison.Ordinal);
        var passwordMatches = string.Equals(expectedPassword, password, StringComparison.Ordinal);

        if (usernameMatches && passwordMatches)
        {
            _authenticatedClients[clientId] = true;
            return Ok;
        }

        return WrongPass;
    }

    private string HandleSet(Guid clientId, IReadOnlyList<string> args)
    {
        if (args.Count != 3)
        {
            return WrongArguments("set");
        }

        if (!IsAuthorized(clientId))
        {
            return NoAuth;
        }

        _store.Set(args[1], args[2]);
        return Ok;
    }

    private string HandleGet(Guid clientId, IReadOnlyList<string> args)
    {
        if (args.Count != 2)
        {
            return WrongArguments("get");
        }

        if (!IsAuthorized(clientId))
        {
            return NoAuth;
        }

        var key = args[1];
        if (!_store.TryGet(key, out var value) || value is null)
        {
            return NullBulkString;
        }

        return $"${value.Length}\r\n{value}\r\n";
    }

    private bool IsAuthorized(Guid clientId)
    {
        if (!RequiresAuthentication())
        {
            return true;
        }

        return _authenticatedClients.TryGetValue(clientId, out var isAuthenticated) && isAuthenticated;
    }

    private bool RequiresAuthentication()
    {
        return !string.IsNullOrWhiteSpace(_options.AuthPassword);
    }

    private static string WrongArguments(string command)
    {
        return $"-ERR wrong number of arguments for '{command}' command\r\n";
    }
}
