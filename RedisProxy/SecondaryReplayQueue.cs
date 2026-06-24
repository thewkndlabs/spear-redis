using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace spearedis.RedisProxy;

public static class SecondaryReplayStates
{
  public const string Pending = "pending";
  public const string Retrying = "retrying";
  public const string Succeeded = "succeeded";
  public const string Stale = "stale";
  public const string Invalid = "invalid";
}

public sealed record QueuedSecondaryReplayEvent(
    long Id,
    string Key,
    string Payload,
    string TargetEndpoint,
    long? ExpiryMilliseconds,
    int AttemptCount,
    DateTimeOffset NextAttemptAtUtc,
    string State);

public interface ISecondaryReplayQueueStore
{
  Task InitializeAsync(CancellationToken cancellationToken);

  Task<long> EnqueueAsync(
      string key,
      string payload,
      string targetEndpoint,
      TimeSpan? expiry,
      CancellationToken cancellationToken);

  Task<IReadOnlyList<QueuedSecondaryReplayEvent>> GetDueAsync(int batchSize, DateTimeOffset nowUtc, CancellationToken cancellationToken);

  Task MarkRetryAsync(long id, DateTimeOffset nextAttemptAtUtc, string reason, CancellationToken cancellationToken);

  Task MarkSucceededAsync(long id, CancellationToken cancellationToken);

  Task MarkStaleAsync(long id, string reason, CancellationToken cancellationToken);

  Task MarkInvalidAsync(long id, string reason, CancellationToken cancellationToken);
}

public sealed class SqliteSecondaryReplayQueueStore : ISecondaryReplayQueueStore
{
  private const string TimestampFormat = "O";

  private readonly string _connectionString;
  private readonly SemaphoreSlim _gate = new(1, 1);

  public SqliteSecondaryReplayQueueStore(IOptions<RedisProxyOptions> options)
  {
    var rawPath = options.Value.ReplayQueueSqlitePath;
    var absolutePath = Path.IsPathRooted(rawPath)
        ? rawPath
        : Path.Combine(AppContext.BaseDirectory, rawPath);

    var directory = Path.GetDirectoryName(absolutePath);
    if (!string.IsNullOrWhiteSpace(directory))
    {
      Directory.CreateDirectory(directory);
    }

    _connectionString = new SqliteConnectionStringBuilder
    {
      DataSource = absolutePath,
      Mode = SqliteOpenMode.ReadWriteCreate,
      Cache = SqliteCacheMode.Shared
    }.ToString();
  }

  public async Task InitializeAsync(CancellationToken cancellationToken)
  {
    const string sql = """
            CREATE TABLE IF NOT EXISTS secondary_replay_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                target_endpoint TEXT NOT NULL,
                expiry_ms INTEGER NULL,
                state TEXT NOT NULL,
                attempt_count INTEGER NOT NULL,
                next_attempt_at_utc TEXT NOT NULL,
                last_error TEXT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_secondary_replay_queue_due
                ON secondary_replay_queue(state, next_attempt_at_utc);
            """;

    await WithConnectionAsync(
        async connection =>
        {
          using var command = connection.CreateCommand();
          command.CommandText = sql;
          await command.ExecuteNonQueryAsync(cancellationToken);
        },
        cancellationToken);
  }

  public async Task<long> EnqueueAsync(
      string key,
      string payload,
      string targetEndpoint,
      TimeSpan? expiry,
      CancellationToken cancellationToken)
  {
    const string sql = """
            INSERT INTO secondary_replay_queue
                (key_name, payload, target_endpoint, expiry_ms, state, attempt_count, next_attempt_at_utc, last_error, created_at_utc, updated_at_utc)
            VALUES
                ($key, $payload, $targetEndpoint, $expiryMs, $state, 0, $nextAttemptAtUtc, NULL, $createdAtUtc, $updatedAtUtc);
            SELECT last_insert_rowid();
            """;

    var nowUtc = DateTimeOffset.UtcNow;
    var expiryMs = expiry.HasValue ? (long?)Math.Round(expiry.Value.TotalMilliseconds) : null;

    return await WithConnectionAsync(
        async connection =>
        {
          using var command = connection.CreateCommand();
          command.CommandText = sql;
          command.Parameters.AddWithValue("$key", key);
          command.Parameters.AddWithValue("$payload", payload);
          command.Parameters.AddWithValue("$targetEndpoint", targetEndpoint);
          command.Parameters.AddWithValue("$expiryMs", (object?)expiryMs ?? DBNull.Value);
          command.Parameters.AddWithValue("$state", SecondaryReplayStates.Pending);
          command.Parameters.AddWithValue("$nextAttemptAtUtc", nowUtc.ToString(TimestampFormat, CultureInfo.InvariantCulture));
          command.Parameters.AddWithValue("$createdAtUtc", nowUtc.ToString(TimestampFormat, CultureInfo.InvariantCulture));
          command.Parameters.AddWithValue("$updatedAtUtc", nowUtc.ToString(TimestampFormat, CultureInfo.InvariantCulture));

          var scalar = await command.ExecuteScalarAsync(cancellationToken);
          return Convert.ToInt64(scalar, CultureInfo.InvariantCulture);
        },
        cancellationToken);
  }

  public async Task<IReadOnlyList<QueuedSecondaryReplayEvent>> GetDueAsync(int batchSize, DateTimeOffset nowUtc, CancellationToken cancellationToken)
  {
    const string sql = """
            SELECT id, key_name, payload, target_endpoint, expiry_ms, attempt_count, next_attempt_at_utc, state
            FROM secondary_replay_queue
            WHERE state IN ($pendingState, $retryingState)
              AND next_attempt_at_utc <= $nowUtc
            ORDER BY next_attempt_at_utc ASC, id ASC
            LIMIT $batchSize;
            """;

    return await WithConnectionAsync(
        async connection =>
        {
          var items = new List<QueuedSecondaryReplayEvent>();
          using var command = connection.CreateCommand();
          command.CommandText = sql;
          command.Parameters.AddWithValue("$pendingState", SecondaryReplayStates.Pending);
          command.Parameters.AddWithValue("$retryingState", SecondaryReplayStates.Retrying);
          command.Parameters.AddWithValue("$nowUtc", nowUtc.ToString(TimestampFormat, CultureInfo.InvariantCulture));
          command.Parameters.AddWithValue("$batchSize", batchSize);

          using var reader = await command.ExecuteReaderAsync(cancellationToken);
          while (await reader.ReadAsync(cancellationToken))
          {
            var expiryMs = reader.IsDBNull(4) ? (long?)null : reader.GetInt64(4);
            var nextAttemptAtUtc = DateTimeOffset.Parse(reader.GetString(6), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
            items.Add(new QueuedSecondaryReplayEvent(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    expiryMs,
                    reader.GetInt32(5),
                    nextAttemptAtUtc,
                    reader.GetString(7)));
          }

          return (IReadOnlyList<QueuedSecondaryReplayEvent>)items;
        },
        cancellationToken);
  }

  public Task MarkRetryAsync(long id, DateTimeOffset nextAttemptAtUtc, string reason, CancellationToken cancellationToken)
  {
    const string sql = """
            UPDATE secondary_replay_queue
            SET
                state = $state,
                attempt_count = attempt_count + 1,
                next_attempt_at_utc = $nextAttemptAtUtc,
                last_error = $lastError,
                updated_at_utc = $updatedAtUtc
            WHERE id = $id;
            """;

    return UpdateStateAsync(id, SecondaryReplayStates.Retrying, reason, sql, cancellationToken, nextAttemptAtUtc);
  }

  public Task MarkSucceededAsync(long id, CancellationToken cancellationToken)
  {
    return UpdateTerminalStateAsync(id, SecondaryReplayStates.Succeeded, null, cancellationToken);
  }

  public Task MarkStaleAsync(long id, string reason, CancellationToken cancellationToken)
  {
    return UpdateTerminalStateAsync(id, SecondaryReplayStates.Stale, reason, cancellationToken);
  }

  public Task MarkInvalidAsync(long id, string reason, CancellationToken cancellationToken)
  {
    return UpdateTerminalStateAsync(id, SecondaryReplayStates.Invalid, reason, cancellationToken);
  }

  private Task UpdateTerminalStateAsync(long id, string state, string? reason, CancellationToken cancellationToken)
  {
    const string sql = """
            UPDATE secondary_replay_queue
            SET
                state = $state,
                last_error = $lastError,
                updated_at_utc = $updatedAtUtc
            WHERE id = $id;
            """;

    return UpdateStateAsync(id, state, reason, sql, cancellationToken, null);
  }

  private Task UpdateStateAsync(
      long id,
      string state,
      string? reason,
      string sql,
      CancellationToken cancellationToken,
      DateTimeOffset? nextAttemptAtUtc)
  {
    var nowUtc = DateTimeOffset.UtcNow;

    return WithConnectionAsync(
        async connection =>
        {
          using var command = connection.CreateCommand();
          command.CommandText = sql;
          command.Parameters.AddWithValue("$id", id);
          command.Parameters.AddWithValue("$state", state);
          command.Parameters.AddWithValue("$lastError", (object?)reason ?? DBNull.Value);
          command.Parameters.AddWithValue("$updatedAtUtc", nowUtc.ToString(TimestampFormat, CultureInfo.InvariantCulture));
          if (nextAttemptAtUtc.HasValue)
          {
            command.Parameters.AddWithValue("$nextAttemptAtUtc", nextAttemptAtUtc.Value.ToString(TimestampFormat, CultureInfo.InvariantCulture));
          }

          await command.ExecuteNonQueryAsync(cancellationToken);
        },
        cancellationToken);
  }

  private async Task WithConnectionAsync(Func<SqliteConnection, Task> action, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      await using var connection = new SqliteConnection(_connectionString);
      await connection.OpenAsync(cancellationToken);
      await action(connection);
    }
    finally
    {
      _gate.Release();
    }
  }

  private async Task<T> WithConnectionAsync<T>(Func<SqliteConnection, Task<T>> action, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      await using var connection = new SqliteConnection(_connectionString);
      await connection.OpenAsync(cancellationToken);
      return await action(connection);
    }
    finally
    {
      _gate.Release();
    }
  }
}

public sealed class SecondaryReplayProcessor
{
  private readonly ISecondaryReplayQueueStore _queueStore;
  private readonly SecondaryTopologyManager _secondaryTopology;
  private readonly ILogger<SecondaryReplayProcessor> _logger;

  public SecondaryReplayProcessor(
      ISecondaryReplayQueueStore queueStore,
      SecondaryTopologyManager secondaryTopology,
      ILogger<SecondaryReplayProcessor> logger)
  {
    _queueStore = queueStore;
    _secondaryTopology = secondaryTopology;
    _logger = logger;
  }

  public async Task ReplayDueEventsAsync(int batchSize, TimeSpan retryBackoff, CancellationToken cancellationToken)
  {
    var due = await _queueStore.GetDueAsync(batchSize, DateTimeOffset.UtcNow, cancellationToken);
    if (due.Count == 0)
    {
      return;
    }

    var targets = _secondaryTopology.GetReplicationTargets()
        .ToDictionary(t => t.Endpoint, StringComparer.OrdinalIgnoreCase);

    foreach (var item in due)
    {
      cancellationToken.ThrowIfCancellationRequested();

      if (!targets.TryGetValue(item.TargetEndpoint, out var target) || !target.IsConnected)
      {
        var reason = $"Target endpoint unavailable: {item.TargetEndpoint}";
        await _queueStore.MarkRetryAsync(item.Id, DateTimeOffset.UtcNow.Add(retryBackoff), reason, cancellationToken);
        _logger.LogWarning("Replay deferred for key {Key} on endpoint {Endpoint}: target unavailable", item.Key, item.TargetEndpoint);
        continue;
      }

      if (!ReplayTimestampParser.TryGetTimestamp(item.Payload, out var queuedTimestamp, out var parseError))
      {
        var reason = $"Invalid replay payload timestamp: {parseError}";
        await _queueStore.MarkInvalidAsync(item.Id, reason, cancellationToken);
        _logger.LogWarning("Replay event marked invalid for key {Key} on endpoint {Endpoint}: {Reason}", item.Key, item.TargetEndpoint, reason);
        continue;
      }

      try
      {
        var currentValue = await target.GetStringAsync(item.Key, cancellationToken);
        if (currentValue is not null
            && ReplayTimestampParser.TryGetTimestamp(currentValue, out var currentTimestamp, out _)
            && queuedTimestamp < currentTimestamp)
        {
          var reason = "Queued timestamp is older than target timestamp.";
          await _queueStore.MarkStaleAsync(item.Id, reason, cancellationToken);
          _logger.LogInformation(
              "Replay event marked stale for key {Key} on endpoint {Endpoint}. QueuedTimestamp={QueuedTimestamp}, TargetTimestamp={TargetTimestamp}",
              item.Key,
              item.TargetEndpoint,
              queuedTimestamp,
              currentTimestamp);
          continue;
        }

        TimeSpan? expiry = item.ExpiryMilliseconds.HasValue
            ? TimeSpan.FromMilliseconds(item.ExpiryMilliseconds.Value)
            : null;

        await target.SetStringAsync(item.Key, item.Payload, expiry, cancellationToken);
        await _queueStore.MarkSucceededAsync(item.Id, cancellationToken);
        _logger.LogInformation("Replay succeeded for key {Key} on endpoint {Endpoint}", item.Key, item.TargetEndpoint);
      }
      catch (Exception ex)
      {
        var reason = ex.Message;
        await _queueStore.MarkRetryAsync(item.Id, DateTimeOffset.UtcNow.Add(retryBackoff), reason, cancellationToken);
        _logger.LogWarning(ex, "Replay failed for key {Key} on endpoint {Endpoint}", item.Key, item.TargetEndpoint);
      }
    }
  }
}

public static class ReplayTimestampParser
{
  public static bool TryGetTimestamp(string jsonPayload, out DateTimeOffset timestamp, out string error)
  {
    timestamp = default;
    error = string.Empty;

    if (string.IsNullOrWhiteSpace(jsonPayload))
    {
      error = "Payload is empty.";
      return false;
    }

    try
    {
      using var document = JsonDocument.Parse(jsonPayload);
      if (document.RootElement.ValueKind != JsonValueKind.Object)
      {
        error = "Payload must be a JSON object.";
        return false;
      }

      if (!document.RootElement.TryGetProperty("__timestamp__", out var timestampElement))
      {
        error = "Missing __timestamp__.";
        return false;
      }

      if (timestampElement.ValueKind == JsonValueKind.String)
      {
        var raw = timestampElement.GetString();
        if (DateTimeOffset.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out timestamp))
        {
          timestamp = timestamp.ToUniversalTime();
          return true;
        }

        error = "Invalid string timestamp format.";
        return false;
      }

      if (timestampElement.ValueKind == JsonValueKind.Number && timestampElement.TryGetInt64(out var value))
      {
        timestamp = value >= 1_000_000_000_000
            ? DateTimeOffset.FromUnixTimeMilliseconds(value)
            : DateTimeOffset.FromUnixTimeSeconds(value);
        return true;
      }

      error = "Unsupported timestamp JSON type.";
      return false;
    }
    catch (Exception ex)
    {
      error = ex.Message;
      return false;
    }
  }
}
