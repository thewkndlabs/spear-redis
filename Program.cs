using spearedis.RedisProxy;

var builder = WebApplication.CreateBuilder(args);

builder.Services
	.AddOptions<RedisProxyOptions>()
	.Bind(builder.Configuration.GetSection("RedisProxy"))
	.PostConfigure(options =>
	{
		var configuredPort = builder.Configuration.GetValue<int?>("RedisProxyPort")
			?? builder.Configuration.GetValue<int?>("redis-proxy-port")
			?? TryParsePort(builder.Configuration["REDIS_PROXY_PORT"]);

		if (configuredPort.HasValue)
		{
			options.Port = configuredPort.Value;
		}
	});
builder.Services.AddSingleton<IStringKeyValueStore, InMemoryStringKeyValueStore>();
builder.Services.AddSingleton<RedisCommandProcessor>();
builder.Services.AddHostedService<RedisProxyHostedService>();

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();

static int? TryParsePort(string? value)
{
	if (string.IsNullOrWhiteSpace(value))
	{
		return null;
	}

	if (int.TryParse(value, out var port))
	{
		return port;
	}

	throw new InvalidOperationException("REDIS_PROXY_PORT must be a valid integer.");
}
