import { createClient } from "redis";

type Mode = "mixed" | "write" | "read";

type Config = {
  connectionString: string;
  mode: Mode;
  clients: number;
  pipeline: number;
  durationSec: number;
  maxOps: number;
  readPercent: number;
  keyPrefix: string;
  keyspace: number;
  valueSize: number;
  reportIntervalMs: number;
};

type Stats = {
  startedAt: number;
  totalOps: number;
  readOps: number;
  writeOps: number;
  errors: number;
  latencySamplesMs: number[];
};

if (Bun.argv.includes("--help")) {
  printHelp();
  process.exit(0);
}

const config = parseConfig();
const stats: Stats = {
  startedAt: Date.now(),
  totalOps: 0,
  readOps: 0,
  writeOps: 0,
  errors: 0,
  latencySamplesMs: [],
};

const hardDeadline = Date.now() + config.durationSec * 1000;
const reportTimer = setInterval(() => {
  printStats(config, stats, false);
}, config.reportIntervalMs);

const results = await Promise.allSettled(
  Array.from({ length: config.clients }, (_, index) => runWorker(index, config, stats, hardDeadline)),
);

clearInterval(reportTimer);
for (const result of results) {
  if (result.status === "rejected") {
    stats.errors += 1;
    console.error(String(result.reason));
  }
}
printStats(config, stats, true);

async function runWorker(
  workerId: number,
  workerConfig: Config,
  workerStats: Stats,
  deadline: number,
): Promise<void> {
  const client = createClient({ url: workerConfig.connectionString });
  const payload = buildPayload(workerConfig.valueSize);

  await client.connect();

  try {
    while (!shouldStop(workerConfig, workerStats, deadline)) {
      const commands = buildBatch(workerId, workerConfig, payload, workerStats.totalOps);
      const started = performance.now();
      const results = await Promise.allSettled(
        commands.map((command) =>
          command.kind === "read"
            ? client.get(command.key)
            : client.set(command.key, payload),
        ),
      );
      const elapsedMs = performance.now() - started;

      applyResults(workerStats, commands, results, elapsedMs);
    }
  } finally {
    client.destroy();
  }
}

function shouldStop(config: Config, stats: Stats, deadline: number): boolean {
  if (Date.now() >= deadline) {
    return true;
  }

  return config.maxOps > 0 && stats.totalOps >= config.maxOps;
}

function buildBatch(workerId: number, config: Config, payload: string, opSeed: number) {
  const commands: Array<{ kind: "read" | "write"; key: string; payload?: string }> = [];

  for (let i = 0; i < config.pipeline; i += 1) {
    const keyIndex = Math.abs((workerId * 1_000_003 + opSeed + i) % config.keyspace);
    const key = `${config.keyPrefix}:${keyIndex}`;
    const pickRead = config.mode === "read"
      || (config.mode === "mixed" && Math.random() * 100 < config.readPercent);

    if (pickRead) {
      commands.push({ kind: "read", key });
    } else {
      commands.push({ kind: "write", key, payload });
    }
  }

  return commands;
}

function applyResults(
  stats: Stats,
  commands: Array<{ kind: "read" | "write"; key: string }>,
  results: PromiseSettledResult<string | null>[],
  elapsedMs: number,
): void {
  const latencyPerOp = elapsedMs / commands.length;
  stats.latencySamplesMs.push(latencyPerOp);

  for (let i = 0; i < commands.length; i += 1) {
    const command = commands[i];
    const result = results[i];

    stats.totalOps += 1;

    if (command.kind === "read") {
      stats.readOps += 1;
    } else {
      stats.writeOps += 1;
    }

    if (result.status === "rejected") {
      stats.errors += 1;
      continue;
    }

    if (command.kind === "write" && result.value !== "OK") {
      stats.errors += 1;
    }
  }
}

function printStats(config: Config, stats: Stats, final: boolean): void {
  const elapsedSec = Math.max(1, (Date.now() - stats.startedAt) / 1000);
  const opsPerSec = stats.totalOps / elapsedSec;
  const avgLatency = average(stats.latencySamplesMs);
  const p95Latency = percentile(stats.latencySamplesMs, 0.95);
  const label = final ? "final" : "progress";

  console.log(
    JSON.stringify({
      label,
      target: config.connectionString,
      mode: config.mode,
      clients: config.clients,
      pipeline: config.pipeline,
      totalOps: stats.totalOps,
      writeOps: stats.writeOps,
      readOps: stats.readOps,
      errors: stats.errors,
      elapsedSec: Number(elapsedSec.toFixed(1)),
      opsPerSec: Number(opsPerSec.toFixed(1)),
      avgLatencyMs: Number(avgLatency.toFixed(2)),
      p95LatencyMs: Number(p95Latency.toFixed(2)),
    }),
  );
}

function average(values: number[]): number {
  if (values.length === 0) {
    return 0;
  }

  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function percentile(values: number[], ratio: number): number {
  if (values.length === 0) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * ratio));
  return sorted[index];
}

function buildPayload(size: number): string {
  const chunk = "0123456789abcdefghijklmnopqrstuvwxyz";
  let payload = "";

  while (payload.length < size) {
    payload += chunk;
  }

  return payload.slice(0, size);
}

function parseConfig(): Config {
  const args = parseArgs(Bun.argv.slice(2));
  const mode = getString(args, "mode", process.env.REDIS_STRESS_MODE ?? "mixed");

  if (mode !== "mixed" && mode !== "write" && mode !== "read") {
    throw new Error("mode must be one of: mixed, write, read");
  }

  const connectionString = getOptionalString(
    args,
    "connection-string",
    process.env.REDIS_URL ?? process.env.REDIS_CONNECTION_STRING,
  );

  if (!connectionString) {
    throw new Error("connection-string is required. Pass --connection-string or set REDIS_URL.");
  }

  const config: Config = {
    connectionString,
    mode,
    clients: getInt(args, "clients", process.env.REDIS_STRESS_CLIENTS ?? "8"),
    pipeline: getInt(args, "pipeline", process.env.REDIS_STRESS_PIPELINE ?? "16"),
    durationSec: getInt(args, "duration", process.env.REDIS_STRESS_DURATION_SEC ?? "30"),
    maxOps: getInt(args, "max-ops", process.env.REDIS_STRESS_MAX_OPS ?? "0"),
    readPercent: getInt(args, "read-percent", process.env.REDIS_STRESS_READ_PERCENT ?? "50"),
    keyPrefix: getString(args, "key-prefix", process.env.REDIS_STRESS_KEY_PREFIX ?? "stress"),
    keyspace: getInt(args, "keyspace", process.env.REDIS_STRESS_KEYSPACE ?? "10000"),
    valueSize: getInt(args, "value-size", process.env.REDIS_STRESS_VALUE_SIZE ?? "256"),
    reportIntervalMs: getInt(args, "report-ms", process.env.REDIS_STRESS_REPORT_MS ?? "2000"),
  };

  if (config.clients <= 0 || config.pipeline <= 0 || config.keyspace <= 0 || config.valueSize <= 0) {
    throw new Error("clients, pipeline, keyspace, and value-size must be positive integers");
  }

  if (config.readPercent < 0 || config.readPercent > 100) {
    throw new Error("read-percent must be between 0 and 100");
  }

  return config;
}

function parseArgs(argv: string[]): Map<string, string> {
  const args = new Map<string, string>();

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      continue;
    }

    const key = token.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith("--")) {
      args.set(key, "true");
      continue;
    }

    args.set(key, value);
    i += 1;
  }

  return args;
}

function getString(args: Map<string, string>, key: string, fallback: string): string {
  return args.get(key) ?? fallback;
}

function getOptionalString(args: Map<string, string>, key: string, fallback?: string): string | undefined {
  const value = args.get(key) ?? fallback;
  return value && value.length > 0 ? value : undefined;
}

function getInt(args: Map<string, string>, key: string, fallback: string): number {
  const raw = args.get(key) ?? fallback;
  const parsed = Number.parseInt(raw, 10);

  if (!Number.isFinite(parsed)) {
    throw new Error(`${key} must be an integer`);
  }

  return parsed;
}

function printHelp(): void {
  console.log(`Usage: bun run stress:redis -- --connection-string <redis-url> [options]

Options:
  --connection-string <url>      Redis connection string, e.g. redis://localhost:6379
  --mode <mixed|write|read>      Operation mix (default: mixed)
  --clients <number>             Concurrent connections (default: 8)
  --pipeline <number>            Concurrent commands per client (default: 16)
  --duration <seconds>           Total run time (default: 30)
  --max-ops <number>             Stop after this many operations (default: 0, unlimited)
  --read-percent <0-100>         Read ratio in mixed mode (default: 50)
  --key-prefix <prefix>          Key prefix (default: stress)
  --keyspace <number>            Distinct keys to target (default: 10000)
  --value-size <bytes>           SET payload size (default: 256)
  --report-ms <ms>               Progress report interval (default: 2000)
  --help                         Show this message
`);
}
