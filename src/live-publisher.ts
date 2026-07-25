/**
 * Live dashboard data publisher.
 * Reads zai_usage.db + api_burn.db and writes ~/nsites/kalman-data/live/data.json
 * Also publishes as NIP-78 compressed events for Nostr-native consumption.
 *
 * Output format matches what live/index.html expects.
 */
import { Database } from "bun:sqlite";

const ZAI_DB = "/home/c03rad0r/.hermes/bot/zai_usage.db";
const BURN_DB = "/home/c03rad0r/.hermes/bot/api_burn.db";
const OUTPUT_PATH = "/home/c03rad0r/nsites/kalman-data/live/data.json";
const HOURS_BACK = 168; // 7 days

// Cost model
const OLLAMA_MONTHLY = 100; // USD
const FRIEND_PREMIUM = 1.21;
const BTC_USD = 95000;
const SATS_PER_USD = 100_000_000 / BTC_USD;

const db = new Database(ZAI_DB, { readonly: true });

function iso(ts: number) { return new Date(ts * 1000).toISOString(); }

// --- Usage by key (7 days) ---
const usageByKey = db.query(`
  SELECT key_name,
    COUNT(*) as calls,
    COALESCE(SUM(total_tokens), 0) as tokens,
    COALESCE(SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END), 0) as successes
  FROM api_calls
  WHERE ts > unixepoch() - ${HOURS_BACK * 3600} AND key_name IS NOT NULL
  GROUP BY key_name
`).all().map((r: any) => ({
  key: r.key_name,
  calls: r.calls,
  tokens: r.tokens,
  success_rate: r.calls > 0 ? Math.round(r.successes / r.calls * 100) : 0,
}));

// --- Daily usage (7 days, aggregated to daily per key) ---
const hourly = db.query(`
  SELECT CAST(ts / 86400 AS INTEGER) * 86400 as day_ts,
    key_name,
    COUNT(*) as calls,
    COALESCE(SUM(total_tokens), 0) as tokens
  FROM api_calls
  WHERE ts > unixepoch() - ${HOURS_BACK * 3600} AND key_name IS NOT NULL
  GROUP BY day_ts, key_name
  ORDER BY day_ts ASC
`).all().map((r: any) => ({
  ts: r.day_ts,
  iso: iso(r.day_ts),
  key: r.key_name,
  calls: r.calls,
  tokens: r.tokens,
}));

// --- Cost breakdown per key ---
function getOllamaRatePerM(): number {
  const row = db.query(`
    SELECT COALESCE(SUM(total_tokens), 0) as tokens
    FROM api_calls
    WHERE key_name = 'ollama_cloud'
    AND ts > unixepoch() - 2592000
  `).get() as any;
  if (!row || row.tokens === 0) return 2.0; // fallback $2/M
  return OLLAMA_MONTHLY / (row.tokens / 1e6);
}

const ollamaRate = getOllamaRatePerM();
const friendRate = ollamaRate * FRIEND_PREMIUM;

function rateForKey(key: string): number {
  if (key === "friend") return friendRate;
  if (key === "ollama_cloud") return ollamaRate;
  if (key === "ours") return ollamaRate;
  if (key === "ppq") return ollamaRate * 0.8; // approximate
  if (key === "openrouter") return ollamaRate;
  return ollamaRate;
}

const costsByKey = usageByKey.map((k: any) => {
  const rate = rateForKey(k.key);
  return {
    key: k.key,
    tokens_M: Math.round(k.tokens / 1e6 * 100) / 100,
    total_usd: Math.round(k.tokens / 1e6 * rate * 100) / 100,
    usd_per_M: Math.round(rate * 100) / 100,
  };
});

const totalUsd7d = costsByKey.reduce((s: number, c: any) => s + c.total_usd, 0);

// --- Kalman data (last 10h, both windows) ---
const kalmanRows = db.query(`
  SELECT ts, key, window, projected_total_pct, used_pct_observed
  FROM kalman_samples
  WHERE ts > unixepoch() - 36000
  ORDER BY ts ASC
  LIMIT 500
`).all().map((r: any) => ({
  ts: r.ts,
  iso: iso(r.ts),
  key: r.key,
  window: r.window,
  projected_pct: r.projected_total_pct,
  used_pct: r.used_pct_observed,
}));

// --- Key decisions timeline (transitions) ---
const transitions = db.query(`
  WITH lagged AS (
    SELECT ts, chosen_key, reason,
      LAG(chosen_key) OVER (ORDER BY ts) as prev_key
    FROM key_decisions
    WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
  )
  SELECT ts, prev_key, chosen_key, reason
  FROM lagged
  WHERE prev_key IS NOT NULL AND chosen_key != prev_key
  ORDER BY ts ASC
  LIMIT 100
`).all().map((r: any) => ({
  ts: r.ts,
  iso: iso(r.ts),
  key: r.chosen_key,
  from: r.prev_key,
  reason: r.reason || "",
}));

// --- Model decisions (recent 50, no time filter — may be old) ---
const decisions = db.query(`
  SELECT ts, model, tier, reason
  FROM model_decisions
  ORDER BY ts DESC
  LIMIT 50
`).all().map((r: any) => ({
  ts: r.ts,
  iso: iso(r.ts),
  model: r.model || "",
  tier: r.tier || "",
  reason: r.reason || "",
  count: 1,
}));

// --- System stats (live from /proc, not from stale DB) ---
import { readFileSync } from "fs";

function readProc(path: string): string {
  try { return readFileSync(path, "utf-8").trim(); } catch { return ""; }
}

const memInfo = readProc("/proc/meminfo");
const memLines = memInfo.split("\n");
const memTotal = parseInt((memLines.find(l => l.startsWith("MemTotal:")) || "").replace(/\D/g, "")) || 16 * 1024 * 1024;
const memAvail = parseInt((memLines.find(l => l.startsWith("MemAvailable:")) || "").replace(/\D/g, "")) || 0;
const memUsed = memTotal - memAvail;
const memPct = Math.round(memUsed / memTotal * 100);

const swapTotal = parseInt((memLines.find(l => l.startsWith("SwapTotal:")) || "").replace(/\D/g, "")) || 0;
const swapFree = parseInt((memLines.find(l => l.startsWith("SwapFree:")) || "").replace(/\D/g, "")) || 0;
const swapUsed = swapTotal - swapFree;
const swapPct = swapTotal > 0 ? Math.round(swapUsed / swapTotal * 100) : 0;

const loadAvg = readProc("/proc/loadavg").split(" ");
const cpuCount = parseInt(readProc("/proc/cpuinfo").split("\n").filter(l => l.startsWith("processor")).length.toString()) || 8;
const cpuPct = Math.round((parseFloat(loadAvg[0]) / cpuCount) * 100);

// Uptime
const uptimeSec = parseInt(readProc("/proc/uptime").split(" ")[0]) || 0;
const uptimeDays = Math.floor(uptimeSec / 86400);

// Get hostname
const hostname = readProc("/etc/hostname") || "DQ05";

const system = {
  cpu_percent: cpuPct,
  cpu_count: cpuCount,
  load_avg: {
    "1m": parseFloat(loadAvg[0]) || 0,
    "5m": parseFloat(loadAvg[1]) || 0,
    "15m": parseFloat(loadAvg[2]) || 0,
  },
  memory: {
    used_pct: memPct,
    used_bytes: memUsed * 1024,
    total_bytes: memTotal * 1024,
  },
  swap: {
    used_pct: swapPct,
    used_bytes: swapUsed * 1024,
    total_bytes: swapTotal * 1024,
  },
  uptime_days: uptimeDays,
  hostname,
};

// System history from DB (last 6h, or all available if less)
const sysRows = db.query(`
  SELECT ts, load_per_core, mem_pct, swap_kb
  FROM system_readings
  ORDER BY ts DESC
  LIMIT 200
`).all();

const systemHistory = sysRows.slice().reverse().map((r: any) => ({
  ts: r.ts,
  iso: iso(r.ts),
  cpu: Math.round(r.load_per_core / cpuCount * 100),
  mem: Math.round(r.mem_pct),
  swap: r.swap_kb > 0 ? Math.min(100, Math.round(r.swap_kb / (swapTotal || 16 * 1024) * 100)) : 0,
}));

// --- Build final JSON ---
const output = {
  usage_by_key: usageByKey,
  hourly,
  costs: {
    total_usd_7d: Math.round(totalUsd7d * 100) / 100,
    ollama_usd_per_M: Math.round(ollamaRate * 100) / 100,
    friend_usd_per_M: Math.round(friendRate * 100) / 100,
    by_key: costsByKey,
  },
  kalman: kalmanRows,
  transitions,
  decisions,
  system: {
    ...system,
    history: systemHistory,
  },
  generated_at: new Date().toISOString(),
};

// Write to file
Bun.write(OUTPUT_PATH, JSON.stringify(output));
console.log(`[publisher] Written ${OUTPUT_PATH} (${JSON.stringify(output).length} bytes)`);
console.log(`  Usage: ${usageByKey.length} keys, Hourly: ${hourly.length} rows`);
console.log(`  Costs: $${totalUsd7d.toFixed(2)} total`);
console.log(`  Kalman: ${kalmanRows.length} samples`);
console.log(`  Transitions: ${transitions.length}, Decisions: ${decisions.length}`);
console.log(`  System: CPU ${system.cpu_percent}%, Mem ${system.memory.used_pct}%, Swap ${system.swap.used_pct}%`);

db.close();