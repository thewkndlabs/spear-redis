using Microsoft.Extensions.Options;
using RedisResp;

namespace spearedis.RedisProxy;

public sealed class RedisProxyHostedService : IHostedService, IDisposable
{
    private readonly RedisCommandProcessor _commandProcessor;
    private readonly IRedisUpstreamClient _upstreamClient;
    private readonly ILogger<RedisProxyHostedService> _logger;
    private readonly RedisProxyOptions _options;

    private RespListener? _listener;
    private RespInterface? _respInterface;

    public RedisProxyHostedService(
        RedisCommandProcessor commandProcessor,
        IRedisUpstreamClient upstreamClient,
        IOptions<RedisProxyOptions> options,
        ILogger<RedisProxyHostedService> logger)
    {
        _commandProcessor = commandProcessor;
        _upstreamClient = upstreamClient;
        _logger = logger;
        _options = options.Value;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _upstreamClient.InitializeAsync(cancellationToken);

        _listener = new RespListener(_options.Port);
        _respInterface = new RespInterface(_listener)
        {
            ArrayHandler = HandleArray
        };

        _respInterface.ClientDisconnected += OnClientDisconnected;
        _respInterface.ErrorOccurred += OnErrorOccurred;

        await _listener.StartAsync(cancellationToken);

        _logger.LogInformation("Redis RESP proxy started on port {Port}", _options.Port);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _listener?.Stop();
        _logger.LogInformation("Redis RESP proxy stopped");
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (_respInterface is not null)
        {
            _respInterface.ClientDisconnected -= OnClientDisconnected;
            _respInterface.ErrorOccurred -= OnErrorOccurred;
            _respInterface.Dispose();
        }

        _listener?.Dispose();
        _upstreamClient.Dispose();
    }

    private string HandleArray(RespDataReceivedEventArgs args)
    {
        if (!TryGetCommandArguments(args.Value, out var commandArgs))
        {
            return "-ERR unknown command ''\r\n";
        }

        return _commandProcessor.Handle(args.ClientGUID, commandArgs);
    }

    private void OnClientDisconnected(object? sender, ClientDisconnectedEventArgs args)
    {
        _commandProcessor.RemoveClient(args.GUID);
    }

    private void OnErrorOccurred(object? sender, RedisResp.ErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Redis RESP proxy error: {Message}", args.Message);
    }

    private static bool TryGetCommandArguments(object? value, out string[] arguments)
    {
        arguments = [];

        if (value is object[] arrayValues)
        {
            arguments = arrayValues
                .Select(v => v?.ToString() ?? string.Empty)
                .ToArray();
            return true;
        }

        var valueType = value?.GetType();
        var dataProperty = valueType?.GetProperty("Data");
        if (dataProperty?.GetValue(value) is object[] propertyValues)
        {
            arguments = propertyValues
                .Select(v => v?.ToString() ?? string.Empty)
                .ToArray();
            return true;
        }

        return false;
    }
}
