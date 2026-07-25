// Raw Nostr CVM client test - bypasses SDK transport issues
// Uses nostr-tools directly to implement the exact wire protocol
import { finalizeEvent, generateSecretKey, getPublicKey, nip44 } from 'nostr-tools';
import { Relay } from 'nostr-tools/relay';

const CLIENT_HEX = process.env.TEST_CLIENT_HEX;
if (!CLIENT_HEX) {
  throw new Error("TEST_CLIENT_HEX env var is required (64-hex nostr secret key)");
}
const SERVER_PUBKEY = '391dafee84361ba41419f217d3482b95b4098ce24680b4ede49882f2f11a9b44';
const RELAYS = ['wss://nostr.mom', 'wss://relay.primal.net'];

const clientSk = new Uint8Array(CLIENT_HEX.match(/.{2}/g).map(b => parseInt(b, 16)));
const clientPk = getPublicKey(clientSk);
let reqId = 0;
const pending = new Map();

async function connectAndTest() {
  console.log(`[CVM-TEST] Client pubkey: ${clientPk}`);
  
  const relays = [];
  for (const url of RELAYS) {
    try {
      const relay = await Relay.connect(url);
      console.log(`[CVM-TEST] Connected to ${url}`);
      
      // Subscribe to gift-wrapped responses
      relay.subscribe(
        [{ kinds: [1059, 21059], '#p': [clientPk], limit: 100 }],
        {
          onevent: (event) => {
            try {
              // Decrypt gift wrap
              const convKey = nip44.v2.utils.getConversationKey(clientSk, event.pubkey);
              const decrypted = nip44.v2.decrypt(event.content, convKey);
              const innerEvent = JSON.parse(decrypted);
              const mcpMsg = JSON.parse(innerEvent.content);
              
              console.log(`[CVM-TEST] Received response for req ${mcpMsg.id}`);
              
              if (mcpMsg.id && pending.has(mcpMsg.id)) {
                const { resolve, timeout } = pending.get(mcpMsg.id);
                clearTimeout(timeout);
                pending.delete(mcpMsg.id);
                
                let resultData = mcpMsg.result;
                if (resultData?.content) {
                  for (const item of resultData.content) {
                    if (item.type === 'text') {
                      try { resultData = JSON.parse(item.text); } catch(e) {}
                    }
                  }
                }
                resolve(resultData);
              }
            } catch (e) {
              console.warn('[CVM-TEST] Failed to handle event:', e.message);
            }
          },
          oneose: () => console.log(`[CVM-TEST] EOSE from ${url}`),
        }
      );
      relays.push(relay);
    } catch (e) {
      console.warn(`[CVM-TEST] Failed to connect ${url}: ${e.message}`);
    }
  }
  
  if (!relays.length) {
    console.error('[CVM-TEST] No relay connections! Aborting.');
    process.exit(1);
  }
  
  // Wait for subscriptions to settle
  console.log('[CVM-TEST] Waiting 2s for subscriptions...');
  await new Promise(r => setTimeout(r, 2000));
  
  // Call get_activity_status
  console.log('\n[CVM-TEST] Calling get_activity_status...');
  try {
    const result = await callTool(relays, 'get_activity_status', {});
    console.log('[CVM-TEST] RESULT:', JSON.stringify(result, null, 2));
  } catch (e) {
    console.error('[CVM-TEST] Tool call failed:', e.message);
  }
  
  // Call get_usage_summary
  console.log('\n[CVM-TEST] Calling get_usage_summary...');
  try {
    const result = await callTool(relays, 'get_usage_summary', { hours_back: 24, bucket_minutes: 60 });
    console.log('[CVM-TEST] RESULT (first 500 chars):', JSON.stringify(result).substring(0, 500));
  } catch (e) {
    console.error('[CVM-TEST] Tool call failed:', e.message);
  }
  
  console.log('\n[CVM-TEST] Done. Closing relays.');
  for (const r of relays) r.close();
  process.exit(0);
}

function callTool(relays, toolName, args = {}) {
  return new Promise((resolve, reject) => {
    const id = ++reqId;
    const timeout = setTimeout(() => {
      if (pending.has(id)) {
        pending.delete(id);
        reject(new Error(`Timeout (30s) for ${toolName}`));
      }
    }, 30000);
    
    pending.set(id, { resolve, reject, timeout });
    
    // Build JSON-RPC request
    const mcpRequest = {
      jsonrpc: '2.0',
      method: 'tools/call',
      params: { name: toolName, arguments: args },
      id: id,
    };
    
    // Create inner Nostr event (kind 25910)
    const innerEvent = {
      pubkey: clientPk,
      kind: 25910,
      tags: [['p', SERVER_PUBKEY]],
      content: JSON.stringify(mcpRequest),
      created_at: Math.floor(Date.now() / 1000),
    };
    
    // Sign inner event
    const signedEvent = finalizeEvent(innerEvent, clientSk);
    console.log(`[CVM-TEST] Signed inner event: ${signedEvent.id?.substring(0, 16)}`);
    
    // Gift wrap: encrypt with NIP-44, wrap in kind 1059
    const wrapSk = generateSecretKey();
    const wrapPk = getPublicKey(wrapSk);
    const convKey = nip44.v2.utils.getConversationKey(wrapSk, SERVER_PUBKEY);
    const encrypted = nip44.v2.encrypt(JSON.stringify(signedEvent), convKey);
    
    const giftWrap = {
      kind: 1059,
      content: encrypted,
      tags: [['p', SERVER_PUBKEY]],
      created_at: Math.floor(Date.now() / 1000),
      pubkey: wrapPk,
    };
    
    const signedGiftWrap = finalizeEvent(giftWrap, wrapSk);
    console.log(`[CVM-TEST] Publishing gift wrap: ${signedGiftWrap.id?.substring(0, 16)}`);
    
    // Publish to all relays
    for (const relay of relays) {
      relay.publish(signedGiftWrap).catch(e => console.warn(`[CVM-TEST] Publish failed: ${e.message}`));
    }
  });
}

connectAndTest().catch(e => { console.error('FATAL:', e); process.exit(1); });
