/**
 * Hermes Insights CVM Server — Direct Nostr implementation.
 *
 * Exposes zai_usage.db + api_burn.db data as MCP tools over the ContextVM
 * protocol (kind 25910 messages, NIP-59 gift wrap).
 *
 * No SDK transport — direct relay subscription + NIP-44 encryption.
 */
import { Database } from "bun:sqlite";
import { finalizeEvent, generateSecretKey, getPublicKey, nip44 } from "nostr-tools";
import { Relay } from "nostr-tools/relay";

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════
const SERVER_HEX = process.env.SERVER_HEX;
if (!SERVER_HEX) {
  throw new Error("SERVER_HEX env var is required (64-hex nostr secret key)");
}
const RELAYS = ["wss://nostr.mom", "wss://relay.primal.net"];
const ZAI_DB_PATH = "/home/c03rad0r/.hermes/bot/zai_usage.db";
const BURN_DB_PATH = "/home/c03rad0r/.hermes/bot/api_burn.db";

const serverSk = new Uint8Array(SERVER_HEX.match(/.{2}/g).map((b) => parseInt(b, 16)));
const serverPk = getPublicKey(serverSk);

console.log(`[hermes-insights-cvm] Starting...`);
console.log(`  Server pubkey: ${serverPk}`);

// ═══════════════════════════════════════════════════════════════════════════
// DATABASE
// ═══════════════════════════════════════════════════════════════════════════
const zaiDb = new Database(ZAI_DB_PATH, { readonly: true });
const burnDb = new Database(BURN_DB_PATH, { readonly: true });
console.log(`  DBs opened OK`);

// ═══════════════════════════════════════════════════════════════════════════
// TOOL IMPLEMENTATIONS
// ═══════════════════════════════════════════════════════════════════════════

// Cost model: Ollama Cloud $100/mo anchor, friend at 1.21x premium
const OLLAMA_MONTHLY_USD = 100;
const FRIEND_PREMIUM = 1.21;

function getOllamaRatePerM() {
  // Count tokens used via ollama_cloud in last 30 days
  const row = zaiDb.query(`
    SELECT COALESCE(SUM(total_tokens), 0) as total_tokens
    FROM api_calls
    WHERE key_name = 'ollama_cloud'
    AND ts > unixepoch() - 30*86400
  `).get() as any;
  const monthlyTokens = row?.total_tokens || 0;
  if (monthlyTokens === 0) return 0.024; // fallback if no usage
  return OLLAMA_MONTHLY_USD / (monthlyTokens / 1e6);
}

const TOOLS = {
  get_activity_status: (args: any) => {
    const row = zaiDb.query(`
      SELECT COUNT(*) as calls, 
             COALESCE(MAX(ts), 0) as last_ts,
             unixepoch() - COALESCE(MAX(ts), 0) as seconds_since_last
      FROM api_calls
      WHERE ts > unixepoch() - 300
    `).get() as any;
    const last5min = zaiDb.query(`
      SELECT COUNT(*) as calls_5min FROM api_calls WHERE ts > unixepoch() - 300
    `).get() as any;
    return {
      active: row.calls > 0,
      calls_in_last_5min: last5min.calls_5min || 0,
      seconds_since_last_call: row.seconds_since_last || 999999,
      last_call_ts: row.last_ts || 0,
    };
  },

  get_usage_summary: (args: any) => {
    const hours = args.hours_back || 24;
    const bucketMin = args.bucket_minutes || 60;
    const since = `unixepoch() - ${hours * 3600}`;

    const totals = zaiDb.query(`
      SELECT COUNT(*) as total_calls,
             COALESCE(SUM(total_tokens), 0) as total_tokens,
             COALESCE(AVG(duration_ms), 0) as avg_duration_ms
      FROM api_calls WHERE ts > ${since}
    `).get() as any;

    const byKeyModel = zaiDb.query(`
      SELECT key_name, model, COUNT(*) as calls,
             COALESCE(SUM(total_tokens), 0) as tokens,
             COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
             COALESCE(SUM(completion_tokens), 0) as completion_tokens,
             COALESCE(AVG(duration_ms), 0) as avg_duration_ms
      FROM api_calls WHERE ts > ${since}
      GROUP BY key_name, model ORDER BY calls DESC
    `).all();

    const byBucket = zaiDb.query(`
      SELECT (ts / (${bucketMin} * 60)) * ${bucketMin} * 60 as bucket_ts,
             key_name, model, COUNT(*) as calls,
             COALESCE(SUM(total_tokens), 0) as tokens
      FROM api_calls WHERE ts > ${since}
      GROUP BY bucket_ts, key_name ORDER BY bucket_ts DESC LIMIT 500
    `).all();

    return { totals, by_key_model: byKeyModel, by_time_bucket: byBucket };
  },

  get_routing_timeline: (args: any) => {
    const hours = args.hours_back || 24;
    const interval = args.sample_interval || 300;
    const since = `unixepoch() - ${hours * 3600}`;

    const decisions = zaiDb.query(`
      SELECT ts, chosen_key, reason, ours_pct, friend_pct
      FROM key_decisions
      WHERE ts > ${since}
      ORDER BY ts DESC
      LIMIT 2000
    `).all();

    return { decisions, count: decisions.length, hours_back: hours };
  },

  get_model_decisions: (args: any) => {
    const hours = args.hours_back || 24;
    const since = `unixepoch() - ${hours * 3600}`;

    const decisions = zaiDb.query(`
      SELECT ts, model, original_model, tier, reason, peak
      FROM model_decisions
      WHERE ts > ${since}
      ORDER BY ts DESC
      LIMIT 2000
    `).all();

    return { decisions, count: decisions.length };
  },

  get_kalman_state: (args: any) => {
    const latest = zaiDb.query(`
      SELECT key, window, used_pct_observed, projected_total_pct,
             burn_rate_per_hour, uncertainty, ts
      FROM kalman_samples
      ORDER BY ts DESC
      LIMIT 100
    `).all();

    const history = zaiDb.query(`
      SELECT key, window, used_pct_observed, projected_total_pct,
             burn_rate_per_hour, ts
      FROM kalman_samples
      WHERE ts > unixepoch() - 86400
      ORDER BY ts ASC
      LIMIT 5000
    `).all();

    return { latest, history_24h: history };
  },

  get_cost_analysis: (args: any) => {
    const days = args.days_back || 30;
    const since = `unixepoch() - ${days * 86400}`;
    const ollamaRate = getOllamaRatePerM();

    const pricingModel: any = {
      ollama_cloud: {
        usd_per_M_tokens: ollamaRate,
        basis: `$${OLLAMA_MONTHLY_USD}/mo subscription, actual 30d usage`,
      },
      friend: {
        usd_per_M_tokens: ollamaRate * FRIEND_PREMIUM,
        basis: `Ollama rate × ${FRIEND_PREMIUM} premium (friend's z.ai)`,
      },
      ours: {
        usd_per_M_tokens: ollamaRate,
        basis: `Ollama rate (our z.ai cancelled, using Ollama anchor)`,
      },
    };

    const keyCosts = zaiDb.query(`
      SELECT key_name,
             COUNT(*) as calls,
             COALESCE(SUM(total_tokens), 0) as tokens
      FROM api_calls WHERE ts > ${since}
      GROUP BY key_name
    `).all() as any[];

    const keyCostsWithPrice = keyCosts.map((k) => {
      const pm = pricingModel[k.key_name] || { usd_per_M_tokens: ollamaRate };
      return {
        ...k,
        effective_cost_usd: (k.tokens / 1e6) * pm.usd_per_M_tokens,
        effective_cost_per_M_tokens: pm.usd_per_M_tokens,
      };
    });

    const totalCost = keyCostsWithPrice.reduce((s, k) => s + k.effective_cost_usd, 0);

    return {
      pricing_model: pricingModel,
      key_costs: keyCostsWithPrice,
      total_effective_cost_usd: totalCost,
      days_back: days,
    };
  },

  get_anomalies: (args: any) => {
    const hours = args.hours_back || 24;
    const since = `unixepoch() - ${hours * 3600}`;

    const anomalies = zaiDb.query(`
      SELECT ts, category, title, detail, severity
      FROM anomaly_events
      WHERE ts > ${since}
      ORDER BY ts DESC
      LIMIT 200
    `).all();

    return { anomalies, count: anomalies.length };
  },

  get_system_stats: (args: any) => {
    const hours = args.hours_back || 6;
    const since = `unixepoch() - ${hours * 3600}`;

    const history = zaiDb.query(`
      SELECT ts, load_per_core, mem_pct, swap_kb, disk_pct
      FROM system_readings
      WHERE ts > ${since}
      ORDER BY ts ASC
      LIMIT 2000
    `).all() as any[];

    // Detect swap thrashing: swap growing rapidly or fluctuating
    let swapAnalysis = { thrashing: false, avg_swap_mb: 0, max_swap_mb: 0, swap_trend: "stable" };
    if (history.length > 10) {
      const swapMbs = history.map(h => (h.swap_kb || 0) / 1024);
      const maxSwap = Math.max(...swapMbs);
      const minSwap = Math.min(...swapMbs);
      const avgSwap = swapMbs.reduce((s: number, v: number) => s + v, 0) / swapMbs.length;
      swapAnalysis = {
        thrashing: maxSwap > avgSwap * 1.5 && (maxSwap - minSwap) > 100,
        avg_swap_mb: Math.round(avgSwap),
        max_swap_mb: Math.round(maxSwap),
        swap_trend: maxSwap > swapMbs[0] ? "growing" : (maxSwap < swapMbs[0] * 0.5 ? "shrinking" : "stable"),
      };
    }

    return { history, swap_analysis: swapAnalysis };
  },

  get_balances: (args: any) => {
    const days = args.days_back || 7;
    const since = `unixepoch() - ${days * 86400}`;

    let history: any[] = [];
    let projections: any[] = [];
    try {
      history = burnDb.query(`
        SELECT ts, provider, balance_usd
        FROM balance_history
        WHERE ts > ${since}
        ORDER BY ts ASC
        LIMIT 1000
      `).all() || [];
    } catch (e) {
      // Table might not exist
    }

    // Calculate projections from history
    const providers = [...new Set(history.map((h: any) => h.provider))];
    for (const prov of providers) {
      const provHistory = history.filter(h => h.provider === prov);
      if (provHistory.length < 2) continue;
      const latest = provHistory[provHistory.length - 1];
      const first = provHistory[0];
      const hoursSpan = (latest.ts - first.ts) / 3600;
      const burnRate = hoursSpan > 0 ? (first.balance_usd - latest.balance_usd) / hoursSpan : 0;
      const hoursUntilDepleted = burnRate > 0 ? latest.balance_usd / burnRate : null;
      projections.push({
        provider: prov,
        current_balance: latest.balance_usd,
        burn_rate_per_hour: burnRate,
        hours_until_depleted: hoursUntilDepleted,
      });
    }

    return { history, projections };
  },

  get_daily_spend: (args: any) => {
    const days = args.days_back || 30;
    const since = `unixepoch() - ${days * 86400}`;
    const ollamaRate = getOllamaRatePerM();

    const daily = zaiDb.query(`
      SELECT date(ts, 'unixepoch') as date,
             key_name,
             COUNT(*) as calls,
             COALESCE(SUM(total_tokens), 0) as tokens
      FROM api_calls WHERE ts > ${since}
      GROUP BY date, key_name ORDER BY date DESC
    `).all() as any[];

    const dailyWithCost = daily.map(d => {
      const rate = d.key_name === 'friend' ? ollamaRate * FRIEND_PREMIUM : ollamaRate;
      return { ...d, effective_cost_usd: (d.tokens / 1e6) * rate };
    });

    return { daily: dailyWithCost, pricing_basis: 'ollama_anchored', ollama_rate_per_M: ollamaRate };
  },
};

// ═══════════════════════════════════════════════════════════════════════════
// TOOL DEFINITIONS (for tools/list)
// ═══════════════════════════════════════════════════════════════════════════
const TOOL_DEFS = Object.keys(TOOLS).map(name => ({
  name,
  description: `Get ${name.replace(/_/g, ' ')} data from the Hermes analytics database`,
  inputSchema: { type: "object", properties: {} },
}));

// ═══════════════════════════════════════════════════════════════════════════
// CVM PROTOCOL HANDLER
// ═══════════════════════════════════════════════════════════════════════════

async function handleMcpMessage(message: any, clientPubkey: string, relays: Relay[]) {
  console.log(`[CVM] Handling: ${message.method} (id=${message.id}) from ${clientPubkey.substring(0, 16)}`);

  let responseContent: any = null;

  if (message.method === "initialize") {
    responseContent = {
      jsonrpc: "2.0",
      id: message.id,
      result: {
        protocolVersion: "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "Hermes Insights", version: "1.0.0" },
      },
    };
  } else if (message.method === "notifications/initialized") {
    // No response needed for notifications
    return;
  } else if (message.method === "tools/list") {
    responseContent = {
      jsonrpc: "2.0",
      id: message.id,
      result: { tools: TOOL_DEFS },
    };
  } else if (message.method === "tools/call") {
    const toolName = message.params?.name;
    const args = message.params?.arguments || {};

    if (!TOOLS[toolName as keyof typeof TOOLS]) {
      responseContent = {
        jsonrpc: "2.0",
        id: message.id,
        error: { code: -32601, message: `Unknown tool: ${toolName}` },
      };
    } else {
      try {
        const result = (TOOLS[toolName as keyof typeof TOOLS] as any)(args);
        responseContent = {
          jsonrpc: "2.0",
          id: message.id,
          result: {
            content: [{ type: "text", text: JSON.stringify(result) }],
          },
        };
        console.log(`[CVM] Tool ${toolName} returned ${JSON.stringify(result).length} bytes`);
      } catch (e: any) {
        responseContent = {
          jsonrpc: "2.0",
          id: message.id,
          error: { code: -32603, message: e.message },
        };
        console.error(`[CVM] Tool ${toolName} error:`, e.message);
      }
    }
  } else {
    responseContent = {
      jsonrpc: "2.0",
      id: message.id,
      error: { code: -32601, message: `Unknown method: ${message.method}` },
    };
  }

  if (!responseContent) return;

  // Send response back to client via gift wrap
  await sendResponse(responseContent, clientPubkey, relays);
}

async function sendResponse(mcpMessage: any, clientPubkey: string, relays: Relay[]) {
  // Create inner event (kind 25910)
  const innerEvent = {
    pubkey: serverPk,
    kind: 25910,
    tags: [["p", clientPubkey]],
    content: JSON.stringify(mcpMessage),
    created_at: Math.floor(Date.now() / 1000),
  };
  const signedEvent = finalizeEvent(innerEvent, serverSk);

  // Gift wrap to client
  const wrapSk = generateSecretKey();
  const wrapPk = getPublicKey(wrapSk);
  const convKey = nip44.v2.utils.getConversationKey(wrapSk, clientPubkey);
  const encrypted = nip44.v2.encrypt(JSON.stringify(signedEvent), convKey);

  const giftWrap = {
    kind: 1059,
    content: encrypted,
    tags: [["p", clientPubkey]],
    created_at: Math.floor(Date.now() / 1000),
    pubkey: wrapPk,
  };
  const signedGiftWrap = finalizeEvent(giftWrap, wrapSk);

  for (const relay of relays) {
    try {
      await relay.publish(signedGiftWrap);
    } catch (e) {
      console.warn(`[CVM] Failed to publish to relay: ${(e as Error).message}`);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN — connect to relays, subscribe, handle requests
// ═══════════════════════════════════════════════════════════════════════════
async function main() {
  const connectedRelays: Relay[] = [];

  for (const url of RELAYS) {
    try {
      console.log(`[CVM] Connecting to ${url}...`);
      const relay = await Promise.race([
        Relay.connect(url),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error("Connection timeout (10s)")), 10000)),
      ]);
      console.log(`[CVM] Connected to ${url}`);

      relay.subscribe(
        [{ kinds: [1059, 21059], limit: 0 }],
        {
          onevent: async (event) => {
            try {
              // Filter client-side: only process events tagged to our server pubkey
              const pTag = event.tags?.find((t: string[]) => t[0] === "p");
              if (!pTag || pTag[1] !== serverPk) return;

              // Decrypt gift wrap
              const convKey = nip44.v2.utils.getConversationKey(serverSk, event.pubkey);
              const decrypted = nip44.v2.decrypt(event.content, convKey);
              const innerEvent = JSON.parse(decrypted);
              const mcpMessage = JSON.parse(innerEvent.content);
              const clientPubkey = innerEvent.pubkey || event.pubkey;

              console.log(`[CVM] Received ${mcpMessage.method} from ${clientPubkey.substring(0, 16)}`);

              // Handle the request
              await handleMcpMessage(mcpMessage, clientPubkey, connectedRelays);
            } catch (e) {
              console.error(`[CVM] Failed to handle event: ${(e as Error).message}`);
            }
          },
          oneose: () => console.log(`[CVM] EOSE from ${url}`),
        }
      );
      connectedRelays.push(relay);
    } catch (e) {
      console.error(`[CVM] Failed to connect ${url}: ${(e as Error).message}`);
    }
  }

  console.log(`[CVM] Server live! Connected to ${connectedRelays.length}/${RELAYS.length} relays`);
  console.log(`[CVM] Pubkey: ${serverPk}`);
  console.log(`[CVM] Tools: ${Object.keys(TOOLS).join(", ")}`);
  console.log(`[CVM] Waiting for requests...`);

  // Keep alive
  setInterval(() => {
    const mem = process.memoryUsage();
    console.log(`[CVM] Heartbeat — RSS: ${Math.round(mem.rss / 1e6)}MB, relays: ${connectedRelays.length}`);
  }, 60000);
}

main().catch((err) => {
  console.error("[CVM] FATAL:", err);
  process.exit(1);
});
