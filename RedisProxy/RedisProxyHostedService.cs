using Microsoft.Extensions.Options;
using RedisResp;
using System.Text;

namespace spearedis.RedisProxy;

public sealed class RedisProxyHostedService : IHostedService, IDisposable
{
    private readonly RedisCommandProcessor _commandProcessor;
    private readonly IRedisUpstreamClient _upstreamClient;
    private readonly ILogger<RedisProxyHostedService> _logger;
    private readonly RedisProxyOptions _options;

    private RespListener? _listener;

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
        _listener.ArrayReceived += OnArrayReceived;
        _listener.ClientDisconnected += OnClientDisconnected;
        _listener.ErrorOccurred += OnErrorOccurred;

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
        if (_listener is not null)
        {
            _listener.ArrayReceived -= OnArrayReceived;
            _listener.ClientDisconnected -= OnClientDisconnected;
            _listener.ErrorOccurred -= OnErrorOccurred;
        }

        _listener?.Dispose();
        _upstreamClient.Dispose();
    }

    private async void OnArrayReceived(object? sender, RespDataReceivedEventArgs args)
    {
        string response;

        if (!TryGetCommandArguments(args.Value, out var commandArgs))
        {
            response = "-ERR unknown command ''\r\n";
        }
        else
        {
            response = _commandProcessor.Handle(args.ClientGUID, commandArgs);
        }

        try
        {
            await SendResponseAsync(args.ClientGUID, response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send RESP response to client {ClientGuid}", args.ClientGUID);
        }
    }

    private async Task SendResponseAsync(Guid clientGuid, string response)
    {
        if (_listener is null)
        {
            return;
        }

        var clientInfo = _listener.RetrieveClientByGuid(clientGuid);
        if (clientInfo?.TcpClient is null || !clientInfo.TcpClient.Connected)
        {
            return;
        }

        var bytes = Encoding.UTF8.GetBytes(response);
        await clientInfo.TcpClient.GetStream().WriteAsync(bytes, 0, bytes.Length);
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
