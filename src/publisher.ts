/**
 * Kalman Dashboard Nostr Publisher — TypeScript (Bun)
 *
 * Reads zai_usage.db + api_burn.db, builds 4 compressed data segments,
 * publishes them as NIP-78 (kind 30078) parameterized replaceable events
 * to wss://nostr.mom.
 *
 * Each event's content is gzip-compressed JSON → base64 encoded.
 * The dashboard decompresses and merges all 4 segments into one data object.
 *
 * Segments (d-tags):
 *   kalman-dash-core   — main chart arrays (times, burn_rate, projected_pct, etc.)
 *   kalman-dash-api    — API hourly sats, key summaries, price info, key transitions
 *   kalman-dash-kalman — per-key Kalman quota windows
 *   kalman-dash-extra  — anomalies, business metrics, routing/model decisions, system stats, balances
 *
 * Usage: bun src/publisher.ts
 */
import { Database } from "bun:sqlite";
import { finalizeEvent, getPublicKey } from "nostr-tools";
import { Relay } from "nostr-tools/relay";
import { gzipSync } from "bun:zlib";
import { Buffer } from "node:buffer";

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════
const SERVER_HEX = process.env.SERVER_HEX;
if (!SERVER_HEX) {
  throw new Error("SERVER_HEX env var is required (64-hex nostr secret key)");
}
const RELAY_URLS = ["wss://nostr.mom", "wss://relay.primal.net"];
const ZAI_DB_PATH = "/home/c03rad0r/.hermes/bot/zai_usage.db";
const BURN_DB_PATH = "/home/c03rad0r/.hermes/bot/api_burn.db";

// Cost model: Ollama Cloud $100/mo anchor, friend at 1.21x premium
const OLLAMA_MONTHLY_USD = 100;
const FRIEND_PREMIUM = 1.21;
const BTC_PRICE_USD = 95000; // Fixed BTC price for SATs conversion
const SATS_PER_BTC = 100_000_000;

// Data limits
const MAX_TIMESERIES_POINTS = 2000;  // Limit kalman_samples timeseries
const MAX_TRANSITIONS = 100;          // Limit key transitions
const MAX_ANOMALIES = 50;             // Limit anomaly events
const HOURS_BACK = 168;               // 7 days for most queries
const QUOTA_HOURS_BACK = 168;         // 7 days for quota windows
const ROUTING_HOURS_BACK = 168;       // 7 days for routing decisions
const MODEL_HOURS_BACK = 168;         // 7 days for model decisions

// ═══════════════════════════════════════════════════════════════════════════
// DATABASE
// ═══════════════════════════════════════════════════════════════════════════
const zaiDb = new Database(ZAI_DB_PATH, { readonly: true });
const burnDb = new Database(BURN_DB_PATH, { readonly: true });

// Server key
const serverSk = new Uint8Array(
  SERVER_HEX.match(/.{2}/g)!.map((b) => parseInt(b, 16))
);
const serverPk = getPublicKey(serverSk);

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

/** Convert a human-readable reason string from key_decisions into a friendlier format */
function humanizeReason(reason: string | null): string {
  if (!reason) return "unknown";
  const r = reason.replace(/_/g, " ");
  // Capitalize first letter
  return r.charAt(0).toUpperCase() + r.slice(1);
}

/** USD per million tokens for a given key_name */
function getOllamaRatePerM(): number {
  const row = zaiDb
    .query(
      `SELECT COALESCE(SUM(total_tokens), 0) as total_tokens
       FROM api_calls
       WHERE key_name = 'ollama_cloud'
       AND ts > unixepoch() - 30*86400`
    )
    .get() as any;
  const monthlyTokens = row?.total_tokens || 0;
  if (monthlyTokens === 0) return 0.024; // fallback
  return OLLAMA_MONTHLY_USD / (monthlyTokens / 1e6);
}

/** Convert USD amount to SATs using fixed BTC price */
function usdToSats(usd: number): number {
  return Math.round((usd / BTC_PRICE_USD) * SATS_PER_BTC);
}

/** Format SATs amount for display (e.g., "12.3k sats") */
function formatSats(sats: number): string {
  if (sats >= 1_000_000) return (sats / 1_000_000).toFixed(1) + "M sats";
  if (sats >= 1_000) return (sats / 1_000).toFixed(1) + "k sats";
  return sats + " sats";
}

/** Compress JSON to gzip then base64 encode */
function compressJson(obj: any): string {
  const jsonStr = JSON.stringify(obj);
  const compressed = gzipSync(Buffer.from(jsonStr, "utf-8"));
  return Buffer.from(compressed).toString("base64");
}

// ═══════════════════════════════════════════════════════════════════════════
// SEGMENT BUILDERS
// ═══════════════════════════════════════════════════════════════════════════

/**
 * SEGMENT 1: kalman-dash-core
 * Main chart arrays from kalman_samples
 */
function buildCoreSegment(): any {
  // Get recent kalman_samples — use the primary key (ours) for main chart
  const rows = zaiDb
    .query(
      `SELECT ts, burn_rate_tph, projected_total_pct, used_pct_observed,
              exhausts_in_hours, will_exhaust
       FROM kalman_samples
       WHERE key = 'friend' AND window = '5-hour'
       AND ts > unixepoch() - ${HOURS_BACK * 3600}
       ORDER BY ts ASC
       LIMIT ${MAX_TIMESERIES_POINTS}`
    )
    .all() as any[];

  const times = rows.map((r) => Math.floor(r.ts * 1000)); // ms timestamps
  const burn_rate = rows.map((r) => r.burn_rate_tph || 0);
  const projected_pct = rows.map((r) => r.projected_total_pct || 0);
  const used_pct = rows.map((r) => r.used_pct_observed || 0);
  const exhausts_in_hours = rows.map((r) => r.exhausts_in_hours ?? null);
  const will_exhaust = rows.map((r) => r.will_exhaust === 1);

  return {
    times,
    burn_rate,
    projected_pct,
    used_pct,
    exhausts_in_hours,
    will_exhaust,
    sample_count: rows.length,
  };
}

/**
 * SEGMENT 2: kalman-dash-api
 * Key transitions, API hourly sats, key summaries, price info
 */
function buildApiSegment(): any {
  const ollamaRate = getOllamaRatePerM();
  const friendRate = ollamaRate * FRIEND_PREMIUM;

  // --- Key transitions: derived from key_decisions where chosen_key changes ---
  // Use SQL LAG window function to find transitions efficiently
  const transitionRows = zaiDb
    .query(
      `WITH lagged AS (
         SELECT ts, chosen_key, reason,
                LAG(chosen_key) OVER (ORDER BY ts) as prev_key
         FROM key_decisions
         WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
       )
       SELECT ts, prev_key, chosen_key, reason
       FROM lagged
       WHERE prev_key IS NOT NULL AND chosen_key != prev_key
       ORDER BY ts ASC
       LIMIT ${MAX_TRANSITIONS}`
    )
    .all() as any[];

  const key_transitions = transitionRows.map((d) => {
    // Find the model for this transition from the nearest model_decision
    const modelRow = zaiDb
      .query(
        `SELECT model FROM model_decisions
         WHERE ts <= ? AND key_name = ?
         ORDER BY ts DESC LIMIT 1`
      )
      .get(d.ts, d.chosen_key) as any;

    return {
      ts: d.ts,
      from: d.prev_key || "",
      to: d.chosen_key || "",
      reason_human: humanizeReason(d.reason),
      model: modelRow?.model || "",
    };
  });

  // --- API hourly sats per provider ---
  // Group api_calls by hour and key_name, compute SATs burned
  const hourlyData = zaiDb
    .query(
      `SELECT (ts / 3600) * 3600 as hour_ts,
              key_name,
              COUNT(*) as calls,
              COALESCE(SUM(total_tokens), 0) as tokens
       FROM api_calls
       WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
       AND key_name IS NOT NULL
       GROUP BY hour_ts, key_name
       ORDER BY hour_ts ASC`
    )
    .all() as any[];

  // Build api_hourly_sats: keyed by provider, each with {times, sats}
  const api_hourly_sats: Record<string, { times: number[]; sats: number[] }> = {};

  // Map key_name to a rate
  function rateForKey(keyName: string): number {
    if (keyName === "friend") return friendRate;
    if (keyName === "ollama_cloud") return ollamaRate;
    if (keyName === "ours") return ollamaRate;
    if (keyName === "ppq") return ollamaRate; // approximate
    if (keyName === "openrouter") return ollamaRate;
    return ollamaRate;
  }

  // Group by key_name
  const byKey: Record<string, any[]> = {};
  for (const h of hourlyData) {
    const key = h.key_name;
    if (!byKey[key]) byKey[key] = [];
    byKey[key].push(h);
  }

  for (const [keyName, rows] of Object.entries(byKey)) {
    const rate = rateForKey(keyName);
    const times = rows.map((r) => r.hour_ts * 1000); // ms
    const sats = rows.map((r) => usdToSats((r.tokens / 1e6) * rate));
    // Normalize key name for display
    const displayName = keyName === "ollama_cloud" ? "ollama" : keyName;
    api_hourly_sats[displayName] = { times, sats };
  }

  // --- Key summaries: per-key call/token/cost/success stats ---
  const keyStats = zaiDb
    .query(
      `SELECT key_name,
              COUNT(*) as calls,
              COALESCE(SUM(total_tokens), 0) as tokens,
              COALESCE(SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END), 0) as successes
       FROM api_calls
       WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
       AND key_name IS NOT NULL
       GROUP BY key_name`
    )
    .all() as any[];

  const key_summaries = keyStats.map((k) => {
    const rate = rateForKey(k.key_name);
    const costUsd = (k.tokens / 1e6) * rate;
    const costSats = usdToSats(costUsd);
    return {
      key: k.key_name,
      calls: k.calls,
      tokens_millions: (k.tokens / 1e6).toFixed(1),
      cost_sats: costSats,
      cost_sats_fmt: formatSats(costSats),
      success_rate: k.calls > 0 ? ((k.successes / k.calls) * 100).toFixed(1) : "0.0",
    };
  });

  // --- Price info ---
  const zaiSatsPerM = usdToSats(ollamaRate);
  const ppqSatsPerM = usdToSats(ollamaRate); // approximate same rate
  const price_info = {
    zai_sats_per_Mtokens: zaiSatsPerM,
    ppq_sats_per_Mtokens: ppqSatsPerM,
    btc_eur: BTC_PRICE_USD, // approximate EUR = USD
    btc_usd: BTC_PRICE_USD,
  };

  return {
    key_transitions,
    api_hourly_sats,
    key_summaries,
    price_info,
  };
}

/**
 * SEGMENT 3: kalman-dash-kalman
 * Per-key Kalman quota windows
 */
function buildKalmanSegment(): any {
  // Get kalman_samples grouped by key and window
  // Keys: "ours", "friend"; Windows: "5-hour", "weekly"
  const keys = ["ours", "friend"];
  const windows = ["5-hour", "weekly"];

  const quota_windows: Record<string, any> = {};

  for (const key of keys) {
    quota_windows[key] = {};
    for (const window of windows) {
      const rows = zaiDb
        .query(
          `SELECT ts, used_pct_observed, projected_total_pct
           FROM kalman_samples
           WHERE key = ? AND window = ?
           AND ts > unixepoch() - ${QUOTA_HOURS_BACK * 3600}
           ORDER BY ts ASC
           LIMIT ${MAX_TIMESERIES_POINTS}`
        )
        .all(key, window) as any[];

      quota_windows[key][window] = {
        times: rows.map((r) => Math.floor(r.ts * 1000)),
        used_pct: rows.map((r) => r.used_pct_observed || 0),
        projected_pct: rows.map((r) => r.projected_total_pct || 0),
      };
    }
  }

  return { quota_windows };
}

/**
 * SEGMENT 4: kalman-dash-extra
 * Anomalies, business metrics, routing/model decisions, system stats, balances
 */
function buildExtraSegment(): any {
  // --- Anomalies ---
  const anomalies = zaiDb
    .query(
      `SELECT ts, severity, category, title, detail
       FROM anomaly_events
       WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
       ORDER BY ts DESC
       LIMIT ${MAX_ANOMALIES}`
    )
    .all() as any[];

  const anomaliesFormatted = anomalies.map((a) => ({
    ts: a.ts,
    severity: a.severity,
    category: a.category,
    explanation: a.title || a.detail || "",
  }));

  // --- Business metrics ---
  // sats_per_task: SATs burned per task completed (hourly)
  // cost_efficiency: {ts, sats_burned, tasks_completed, crash_free_rate}
  // resource_efficiency: {ts, cpu_efficiency, memory_efficiency}
  // crash_events: {ts, crashes}
  // tasks_completed: {ts, value}

  // Get hourly buckets of api_calls for sats burned
  const ollamaRate = getOllamaRatePerM();
  const hourlyTokens = zaiDb
    .query(
      `SELECT (ts / 3600) * 3600 as hour_ts,
              COUNT(*) as calls,
              COALESCE(SUM(total_tokens), 0) as tokens,
              COALESCE(SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END), 0) as successes
       FROM api_calls
       WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
       AND key_name IS NOT NULL
       GROUP BY hour_ts
       ORDER BY hour_ts ASC`
    )
    .all() as any[];

  // Get task_duration_samples for tasks_completed proxy
  // Use task_duration_samples grouped by hour as tasks completed
  const taskSamples = zaiDb
    .query(
      `SELECT (ts / 3600) * 3600 as hour_ts,
              COUNT(*) as task_count
       FROM task_duration_samples
       WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
       AND outcome = 'completed'
       GROUP BY hour_ts
       ORDER BY hour_ts ASC`
    )
    .all() as any[];

  // Build a merged hourly map
  const hourlyMap: Record<number, { sats: number; calls: number; successes: number; tasks: number }> = {};
  for (const h of hourlyTokens) {
    hourlyMap[h.hour_ts] = {
      sats: usdToSats((h.tokens / 1e6) * ollamaRate),
      calls: h.calls,
      successes: h.successes,
      tasks: 0,
    };
  }
  for (const t of taskSamples) {
    if (!hourlyMap[t.hour_ts]) hourlyMap[t.hour_ts] = { sats: 0, calls: 0, successes: 0, tasks: 0 };
    hourlyMap[t.hour_ts].tasks = t.task_count;
  }

  const sortedHours = Object.keys(hourlyMap).map(Number).sort((a, b) => a - b);

  const sats_per_task = sortedHours.map((h) => ({
    ts: h,
    value: hourlyMap[h].tasks > 0 ? Math.round(hourlyMap[h].sats / hourlyMap[h].tasks) : 0,
  }));

  const cost_efficiency = sortedHours.map((h) => ({
    ts: h,
    sats_burned: hourlyMap[h].sats,
    tasks_completed: hourlyMap[h].tasks,
    crash_free_rate: hourlyMap[h].calls > 0
      ? (hourlyMap[h].successes / hourlyMap[h].calls) * 100
      : 100,
  }));

  // Resource efficiency from system_readings
  const sysReadings = zaiDb
    .query(
      `SELECT ts, load_per_core, mem_pct
       FROM system_readings
       WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
       ORDER BY ts ASC
       LIMIT 1000`
    )
    .all() as any[];

  // Estimate CPU efficiency: 100 - load_per_core (clamped 0-100)
  // Memory efficiency: 100 - mem_pct
  const resource_efficiency = sysReadings.map((r) => ({
    ts: Math.floor(r.ts),
    cpu_efficiency: Math.max(0, Math.min(100, 100 - (r.load_per_core || 0) * 10)),
    memory_efficiency: Math.max(0, Math.min(100, 100 - (r.mem_pct || 0))),
  }));

  // Crash events
  const crashes = zaiDb
    .query(
      `SELECT ts, crash_count
       FROM crash_events
       ORDER BY ts ASC`
    )
    .all() as any[];

  // Parse ts which may be a string date like "2026-07-07 22:47:42"
  const crash_events = crashes.map((c) => ({
    ts: typeof c.ts === "string" ? Math.floor(new Date(c.ts + "Z").getTime() / 1000) : c.ts,
    crashes: c.crash_count || 0,
  }));

  const tasks_completed = taskSamples.map((t) => ({
    ts: t.hour_ts,
    value: t.task_count,
  }));

  const business_metrics = {
    sats_per_task,
    cost_efficiency,
    resource_efficiency,
    crash_events,
    tasks_completed,
  };

  // --- Routing decisions (for timeline chart) ---
  const routingDecisions = zaiDb
    .query(
      `SELECT ts, chosen_key, reason, ours_pct, friend_pct
       FROM key_decisions
       WHERE ts > unixepoch() - ${ROUTING_HOURS_BACK * 3600}
       ORDER BY ts DESC
       LIMIT 500`
    )
    .all() as any[];

  const routing_decisions = routingDecisions.map((d) => ({
    ts: d.ts,
    chosen_key: d.chosen_key,
    reason: d.reason,
    ours_pct: d.ours_pct ?? 0,
    friend_pct: d.friend_pct ?? 0,
  }));

  // --- Model decisions ---
  const modelDecisions = zaiDb
    .query(
      `SELECT ts, model, original_model, tier, reason, peak
       FROM model_decisions
       WHERE ts > unixepoch() - ${MODEL_HOURS_BACK * 3600}
       ORDER BY ts DESC
       LIMIT 500`
    )
    .all() as any[];

  const model_decisions = modelDecisions.map((d) => ({
    ts: d.ts,
    model: d.model,
    original_model: d.original_model,
    tier: d.tier,
    reason: d.reason,
    peak: d.peak ?? 0,
  }));

  // --- System stats (from system_readings, includes swap analysis) ---
  const sysHistory = zaiDb
    .query(
      `SELECT ts, load_per_core, mem_pct, swap_kb, api_quota_pct, cpu_smoothed, running_workers, pending_tasks
       FROM system_readings
       WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
       ORDER BY ts ASC
       LIMIT 2000`
    )
    .all() as any[];

  // Swap analysis
  let swap_analysis = { thrashing: false, avg_swap_mb: 0, max_swap_mb: 0, swap_trend: "stable" };
  if (sysHistory.length > 10) {
    const swapMbs = sysHistory.map((h) => (h.swap_kb || 0) / 1024);
    const maxSwap = Math.max(...swapMbs);
    const minSwap = Math.min(...swapMbs);
    const avgSwap = swapMbs.reduce((s: number, v: number) => s + v, 0) / swapMbs.length;
    swap_analysis = {
      thrashing: maxSwap > avgSwap * 1.5 && (maxSwap - minSwap) > 100,
      avg_swap_mb: Math.round(avgSwap),
      max_swap_mb: Math.round(maxSwap),
      swap_trend: maxSwap > swapMbs[0] ? "growing" : maxSwap < swapMbs[0] * 0.5 ? "shrinking" : "stable",
    };
  }

  const system_stats = {
    history: sysHistory.map((r) => ({
      ts: Math.floor(r.ts),
      load_per_core: r.load_per_core,
      mem_pct: r.mem_pct,
      swap_kb: r.swap_kb,
      disk_pct: r.disk_pct,
    })),
    swap_analysis,
  };

  // --- Balances (from api_burn.db) ---
  let balanceHistory: any[] = [];
  let balanceProjections: any[] = [];

  try {
    balanceHistory = burnDb
      .query(
        `SELECT ts, provider, balance_usd
         FROM balance_snapshots
         WHERE ts > unixepoch() - ${HOURS_BACK * 3600}
         AND balance_usd IS NOT NULL
         AND error IS NULL
         ORDER BY ts ASC
         LIMIT 1000`
      )
      .all() as any[];

    // Calculate projections from history
    const providers = [...new Set(balanceHistory.map((h) => h.provider))];
    for (const prov of providers) {
      const provHistory = balanceHistory.filter((h) => h.provider === prov);
      if (provHistory.length < 2) continue;
      const latest = provHistory[provHistory.length - 1];
      const first = provHistory[0];
      const hoursSpan = (latest.ts - first.ts) / 3600;
      const burnRate = hoursSpan > 0 ? (first.balance_usd - latest.balance_usd) / hoursSpan : 0;
      const hoursUntilDepleted = burnRate > 0 ? latest.balance_usd / burnRate : null;
      balanceProjections.push({
        provider: prov,
        current_balance: latest.balance_usd,
        burn_rate_per_hour: burnRate,
        hours_until_depleted: hoursUntilDepleted,
      });
    }
  } catch (e) {
    // Table might not exist or other error
  }

  const balances = {
    history: balanceHistory.map((h) => ({
      ts: h.ts,
      provider: h.provider,
      balance_usd: h.balance_usd,
    })),
    projections: balanceProjections,
  };

  return {
    anomalies: anomaliesFormatted,
    business_metrics,
    routing_decisions,
    model_decisions,
    system_stats,
    balances,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// PUBLISH
// ═══════════════════════════════════════════════════════════════════════════

async function publishSegment(
  relay: Relay,
  dTag: string,
  content: any,
  label: string
): Promise<boolean> {
  // Compress and encode
  const b64 = compressJson(content);

  // Build kind 30078 event
  const eventTemplate = {
    kind: 30078,
    pubkey: serverPk,
    tags: [
      ["d", dTag],
      ["compression", "gzip-b64"],
    ],
    content: b64,
    created_at: Math.floor(Date.now() / 1000),
  };

  // Sign
  const signedEvent = finalizeEvent(eventTemplate, serverSk);

  // Publish
  try {
    await relay.publish(signedEvent);
    console.log(`Published segment: ${dTag} (${b64.length} bytes)`);
    return true;
  } catch (e: any) {
    console.error(`Failed to publish ${dTag}: ${e.message}`);
    return false;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  console.log("[publisher] Starting...");
  console.log(`  Server pubkey: ${serverPk}`);
  console.log(`  Relays: ${RELAY_URLS.join(", ")}`);

  // Build all 4 segments
  console.log("[publisher] Building data segments...");
  const core = buildCoreSegment();
  const api = buildApiSegment();
  const kalman = buildKalmanSegment();
  const extra = buildExtraSegment();

  console.log(
    `  core: ${core.sample_count} samples, api: ${api.key_transitions.length} transitions, ` +
    `kalman: ${Object.keys(kalman.quota_windows).length} keys, extra: ${extra.anomalies.length} anomalies`
  );

  // Publish each segment with retry across relays
  let ok = 0;
  for (const [dTag, data, label] of [
    ["kalman-dash-core", core, "core charts"],
    ["kalman-dash-api", api, "api metrics"],
    ["kalman-dash-kalman", kalman, "kalman by key"],
    ["kalman-dash-extra", extra, "extra + business"],
  ] as const) {
    let published = false;
    for (const relayUrl of RELAY_URLS) {
      console.log(`[publisher] Connecting to ${relayUrl} for ${dTag}...`);
      let relay;
      try {
        relay = await Promise.race([
          Relay.connect(relayUrl),
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error("Connection timeout (8s)")), 8000)
          ),
        ]);
      } catch (e: any) {
        console.error(`[publisher] Connection failed: ${e.message}`);
        continue;
      }
      published = await publishSegment(relay, dTag as string, data as object, label as string);
      try { relay.close(); } catch {}
      if (published) break;
      await new Promise(r => setTimeout(r, 300));
    }
    if (published) ok++;
    await new Promise(r => setTimeout(r, 200));
  }

  console.log(`\n[publisher] Done: ${ok}/4 segments published.`);
  zaiDb.close();
  burnDb.close();
}

main().catch((err) => {
  console.error("[publisher] FATAL:", err);
  process.exit(1);
});