using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace spearedis.RedisProxy;

public sealed class RedisCommandProcessor
{
    private const string Ok = "+OK\r\n";
    private const string Pong = "+PONG\r\n";
    private const string NullBulkString = "$-1\r\n";
    private const string EmptyArray = "*0\r\n";
    private const string NoAuth = "-NOAUTH Authentication required.\r\n";
    private const string WrongPass = "-WRONGPASS invalid username-password pair or user is disabled.\r\n";
    private const string PrimaryWriteFailed = "-ERR primary write failed\r\n";
    private const string PrimaryReadFailed = "-ERR primary read failed\r\n";
    private const string PrimaryKeysFailed = "-ERR primary keys failed\r\n";
    private const string SyntaxError = "-ERR syntax error\r\n";
    private const string InvalidInteger = "-ERR value is not an integer or out of range\r\n";

    private readonly IRedisUpstreamClient _upstream;
    private readonly RedisProxyOptions _options;
    private readonly ILogger<RedisCommandProcessor> _logger;
    private readonly ConcurrentDictionary<Guid, bool> _authenticatedClients = new();

    public RedisCommandProcessor(
        IRedisUpstreamClient upstream,
        IOptions<RedisProxyOptions> options,
        ILogger<RedisCommandProcessor> logger)
    {
        _upstream = upstream;
        _options = options.Value;
        _logger = logger;
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
            "KEYS" => HandleKeys(clientId, arguments),
            "PING" => HandlePing(arguments),
            "ECHO" => HandleEcho(arguments),
            "COMMAND" => HandleCommand(arguments),
            "HELLO" => HandleHello(arguments),
            "CLIENT" => HandleClient(arguments),
            "SELECT" => HandleSelect(arguments),
            "QUIT" => HandleQuit(arguments),
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
        if (args.Count is not (3 or 5))
        {
            return WrongArguments("set");
        }

        if (!IsAuthorized(clientId))
        {
            return NoAuth;
        }

        var key = args[1];
        var value = args[2];

        var parseResult = TryParseSetExpiry(args, out var expiry);
        if (parseResult is not null)
        {
            return parseResult;
        }

        var setResult = _upstream.WriteToPrimary(key, value, expiry);
        if (!setResult.Success)
        {
            return PrimaryWriteFailed;
        }

        var replicationTask = _upstream.ReplicateToSecondariesAsync(key, value, expiry);
        _ = replicationTask.ContinueWith(
            t => _logger.LogWarning(t.Exception, "Secondary replication task failed for key {Key}", key),
            TaskContinuationOptions.OnlyOnFaulted);

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

        var readResult = _upstream.ReadFromPrimary(args[1]);
        if (!readResult.Success)
        {
            return PrimaryReadFailed;
        }

        if (!readResult.Found || readResult.Value is null)
        {
            return NullBulkString;
        }

        var value = readResult.Value;
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

    private string HandleKeys(Guid clientId, IReadOnlyList<string> args)
    {
        if (args.Count != 2)
        {
            return WrongArguments("keys");
        }

        if (!IsAuthorized(clientId))
        {
            return NoAuth;
        }

        var keysResult = _upstream.KeysFromPrimary(args[1]);
        if (!keysResult.Success)
        {
            return PrimaryKeysFailed;
        }

        return ToRespArray(keysResult.Keys);
    }

    private static string HandlePing(IReadOnlyList<string> args)
    {
        return args.Count switch
        {
            1 => Pong,
            2 => ToRespBulkString(args[1]),
            _ => WrongArguments("ping")
        };
    }

    private static string HandleEcho(IReadOnlyList<string> args)
    {
        if (args.Count != 2)
        {
            return WrongArguments("echo");
        }

        return ToRespBulkString(args[1]);
    }

    private static string HandleCommand(IReadOnlyList<string> args)
    {
        if (args.Count == 1)
        {
            return EmptyArray;
        }

        var subCommand = args[1].ToUpperInvariant();
        return subCommand switch
        {
            "COUNT" when args.Count == 2 => ":0\r\n",
            "INFO" or "DOCS" or "GETKEYS" when args.Count >= 2 => EmptyArray,
            _ => UnsupportedSubcommand("command", args[1])
        };
    }

    private static string HandleHello(IReadOnlyList<string> args)
    {
        if (args.Count is < 1 or > 7)
        {
            return WrongArguments("hello");
        }

        var protocol = "3";
        if (args.Count >= 2)
        {
            if (args[1] is not ("2" or "3"))
            {
                return "-ERR NOPROTO unsupported protocol version\r\n";
            }

            protocol = args[1];
        }

        return $"%7\r\n+server\r\n+redis\r\n+version\r\n+7.0.0\r\n+proto\r\n:{protocol}\r\n+id\r\n:1\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n";
    }

    private static string HandleClient(IReadOnlyList<string> args)
    {
        if (args.Count < 2)
        {
            return WrongArguments("client");
        }

        var subCommand = args[1].ToUpperInvariant();
        return subCommand switch
        {
            "SETINFO" when args.Count == 4 => Ok,
            "SETNAME" when args.Count == 3 => Ok,
            "GETNAME" when args.Count == 2 => NullBulkString,
            "ID" when args.Count == 2 => ":1\r\n",
            _ => UnsupportedSubcommand("client", args[1])
        };
    }

    private static string HandleSelect(IReadOnlyList<string> args)
    {
        if (args.Count != 2)
        {
            return WrongArguments("select");
        }

        return Ok;
    }

    private static string HandleQuit(IReadOnlyList<string> args)
    {
        if (args.Count != 1)
        {
            return WrongArguments("quit");
        }

        return Ok;
    }

    private static string ToRespBulkString(string value)
    {
        return $"${value.Length}\r\n{value}\r\n";
    }

    private static string ToRespArray(IReadOnlyList<string> values)
    {
        if (values.Count == 0)
        {
            return EmptyArray;
        }

        var builder = new System.Text.StringBuilder();
        builder.Append('*').Append(values.Count).Append("\r\n");
        foreach (var value in values)
        {
            builder.Append('$').Append(value.Length).Append("\r\n").Append(value).Append("\r\n");
        }

        return builder.ToString();
    }

    private static string UnsupportedSubcommand(string command, string subCommand)
    {
        return $"-ERR unsupported subcommand '{subCommand.ToLowerInvariant()}' for '{command}' command\r\n";
    }

    private static string WrongArguments(string command)
    {
        return $"-ERR wrong number of arguments for '{command}' command\r\n";
    }

    private static string? TryParseSetExpiry(IReadOnlyList<string> args, out TimeSpan? expiry)
    {
        expiry = null;

        if (args.Count == 3)
        {
            return null;
        }

        var option = args[3].ToUpperInvariant();
        if (!long.TryParse(args[4], out var ttlValue) || ttlValue <= 0)
        {
            return InvalidInteger;
        }

        expiry = option switch
        {
            "EX" => TimeSpan.FromSeconds(ttlValue),
            "PX" => TimeSpan.FromMilliseconds(ttlValue),
            _ => null
        };

        return expiry is null ? SyntaxError : null;
    }
}
