using spearedis.RedisProxy;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services
	.AddOptions<RedisProxyOptions>()
	.Bind(builder.Configuration.GetSection("RedisProxy"))
	.ValidateOnStart()
	.PostConfigure(options =>
	{
		var configuredPort = builder.Configuration.GetValue<int?>("RedisProxyPort")
			?? builder.Configuration.GetValue<int?>("redis-proxy-port")
			?? TryParsePort(builder.Configuration["REDIS_PROXY_PORT"]);

		if (configuredPort.HasValue)
		{
			options.Port = configuredPort.Value;
		}

		var primary = builder.Configuration["REDIS_PROXY_PRIMARY_CONNECTION_STRING"]
			?? builder.Configuration["redis-proxy-primary-connection-string"]
			?? builder.Configuration["RedisProxyPrimaryConnectionString"];

		if (!string.IsNullOrWhiteSpace(primary))
		{
			options.PrimaryConnectionString = primary;
		}

		var secondaryListRaw = builder.Configuration["REDIS_PROXY_SECONDARY_CONNECTION_STRINGS"]
			?? builder.Configuration["redis-proxy-secondary-connection-strings"]
			?? builder.Configuration["RedisProxySecondaryConnectionStrings"];

		if (!string.IsNullOrWhiteSpace(secondaryListRaw))
		{
			options.SecondaryConnectionStrings = SplitSecondaryConnectionStrings(secondaryListRaw);
		}

		var singleSecondary = builder.Configuration["REDIS_PROXY_SECONDARY_CONNECTION_STRING"]
			?? builder.Configuration["redis-proxy-secondary-connection-string"]
			?? builder.Configuration["RedisProxySecondaryConnectionString"];

		if (!string.IsNullOrWhiteSpace(singleSecondary))
		{
			options.SecondaryConnectionStrings.Add(singleSecondary);
		}
	});
builder.Services.AddSingleton<IValidateOptions<RedisProxyOptions>, RedisProxyOptionsValidator>();
builder.Services.AddSingleton<ISecondaryRedisClientFactory, StackExchangeSecondaryRedisClientFactory>();
builder.Services.AddSingleton<SecondaryTopologyManager>();
builder.Services.AddSingleton<IRedisUpstreamClient, RedisUpstreamClient>();
builder.Services.AddSingleton<RedisHealthService>();
builder.Services.AddSingleton<RedisCommandProcessor>();
builder.Services.AddHostedService<RedisProxyHostedService>();

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.MapGet("/health/live", (RedisHealthService healthService) =>
{
	return Results.Ok(healthService.GetLiveness());
});

app.MapGet("/health/ready", (RedisHealthService healthService) =>
{
	var readiness = healthService.GetReadiness();
	return readiness.IsReady
		? Results.Ok(readiness.Response)
		: Results.Json(readiness.Response, statusCode: StatusCodes.Status503ServiceUnavailable);
});

app.MapGet("/health/full", (RedisHealthService healthService) =>
{
	return Results.Ok(healthService.GetFull());
});

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

static List<string> SplitSecondaryConnectionStrings(string value)
{
	return value
		.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
		.Where(v => !string.IsNullOrWhiteSpace(v))
		.ToList();
}
