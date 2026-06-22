'use strict';

require('dotenv').config();

const http = require('http');
const { URL } = require('url');
const { WebSocketServer, WebSocket } = require('ws');
const { createClient } = require('@supabase/supabase-js');

const PORT = parseInt(process.env.PORT, 10) || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('[FATAL] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment.');
  process.exit(1);
}

// Service-role client, used only to validate access tokens on connect.
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// ── Limits (the app enforces these too; the server is the backstop) ──────────
const MAX_RACERS = 8;
const MAX_SPECTATORS = 20;

// ── Close codes (4000–4999 = application-defined) ────────────────────────────
const CLOSE_BAD_REQUEST = 4000; // missing/invalid query params, or replaced
const CLOSE_AUTH_FAILED = 4001; // token invalid, expired, or user mismatch
const CLOSE_ROOM_FULL   = 4002; // racer or spectator cap reached

// Events relayed immediately to everyone else in the room (sender excluded).
const FANOUT_EVENTS = new Set([
  'chat', 'voice_msg', 'cheer', 'finished', 'racer_quit', 'false_start', 'race_over',
]);

const BATCH_INTERVAL_MS = 1000;   // GPS fan-out cadence
const PING_INTERVAL_MS  = 30000;  // dead-connection sweep

// roomId -> { clients: Set<ws>, gps: Map<userId, {user_id,distance_m,speed_kmh,ts}>, dirty: boolean }
const rooms = new Map();
const allClients = new Set();

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

function getRoom(roomId) {
  let room = rooms.get(roomId);
  if (!room) {
    room = { clients: new Set(), gps: new Map(), dirty: false };
    rooms.set(roomId, room);
  }
  return room;
}

function countByRole(room, role) {
  let n = 0;
  for (const c of room.clients) if (c.role === role) n++;
  return n;
}

function send(ws, message) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
}

// Send to every client in the room, optionally excluding one (the sender).
function broadcast(room, message, exceptWs) {
  const data = JSON.stringify(message);
  for (const client of room.clients) {
    if (client === exceptWs) continue;
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}

// ── HTTP server: health check + host for the WS upgrade ──────────────────────
// Any plain HTTP GET is a health check. WebSocket upgrades fire the 'upgrade'
// event (handled by ws), not this 'request' handler, so they never reach here —
// matching on method alone is enough and survives Render probing any path,
// trailing slash, or query string.
const server = http.createServer((req, res) => {
  const isUpgrade = (req.headers.upgrade || '').toLowerCase() === 'websocket';
  if (!isUpgrade && (req.method === 'GET' || req.method === 'HEAD')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    // HEAD probes (e.g. `curl -I`, some uptime checkers) want headers only.
    res.end(req.method === 'HEAD' ? undefined : JSON.stringify({ status: 'ok', rooms: rooms.size, connections: allClients.size }));
    return;
  }
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found' }));
});

const wss = new WebSocketServer({ server });

wss.on('connection', async (ws, req) => {
  // ── Parse connection query: ?roomId=X&userId=Y&token=Z&role=racer|spectator
  let params;
  try {
    params = new URL(req.url, `http://${req.headers.host || 'localhost'}`).searchParams;
  } catch (e) {
    ws.close(CLOSE_BAD_REQUEST, 'bad request');
    return;
  }
  const roomId = params.get('roomId');
  const userId = params.get('userId');
  const token  = params.get('token');
  const role   = params.get('role');

  if (!roomId || !userId || !token || (role !== 'racer' && role !== 'spectator')) {
    log('REJECT bad-params', { roomId, userId, role });
    ws.close(CLOSE_BAD_REQUEST, 'missing or invalid query params');
    return;
  }

  // ── Auth: one Supabase call to validate the access token ───────────────────
  try {
    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data || !data.user) {
      log('REJECT auth-failed', userId, error && error.message);
      ws.close(CLOSE_AUTH_FAILED, 'auth failed');
      return;
    }
    if (data.user.id !== userId) {
      log('REJECT token-user-mismatch', { claimed: userId, token: data.user.id });
      ws.close(CLOSE_AUTH_FAILED, 'token/user mismatch');
      return;
    }
  } catch (e) {
    log('REJECT auth-error', userId, e && e.message);
    ws.close(CLOSE_AUTH_FAILED, 'auth error');
    return;
  }

  // The client may have bailed during the async auth call.
  if (ws.readyState !== WebSocket.OPEN) return;

  const room = getRoom(roomId);

  // ── Replace a stale connection for the same user (reconnect / network blip).
  // Remove it from the room before its close handler fires so we don't emit a
  // spurious racer_quit or miscount the caps below.
  for (const c of room.clients) {
    if (c.userId === userId) {
      log('REPLACE existing connection', { roomId, userId });
      room.clients.delete(c);
      allClients.delete(c);
      try { c.close(CLOSE_BAD_REQUEST, 'replaced by new connection'); } catch (e) {}
    }
  }

  // ── Capacity caps ──────────────────────────────────────────────────────────
  if (role === 'racer' && countByRole(room, 'racer') >= MAX_RACERS) {
    log('REJECT room-full racers', { roomId, userId });
    ws.close(CLOSE_ROOM_FULL, 'room full (racers)');
    return;
  }
  if (role === 'spectator' && countByRole(room, 'spectator') >= MAX_SPECTATORS) {
    log('REJECT room-full spectators', { roomId, userId });
    ws.close(CLOSE_ROOM_FULL, 'room full (spectators)');
    return;
  }

  // ── Join ───────────────────────────────────────────────────────────────────
  ws.roomId = roomId;
  ws.userId = userId;
  ws.role = role;
  ws.isAlive = true;
  room.clients.add(ws);
  allClients.add(ws);
  log(`JOIN room=${roomId} user=${userId} role=${role} ` +
      `(racers=${countByRole(room, 'racer')} spectators=${countByRole(room, 'spectator')})`);

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return; // ignore malformed frames
    }
    if (!msg || typeof msg.event !== 'string') return;
    const event = msg.event;
    const payload = msg.payload || {};

    // gps: buffer the racer's latest position; the batch timer fans it out.
    if (event === 'gps') {
      if (ws.role !== 'racer') return; // only racers contribute positions
      room.gps.set(ws.userId, {
        user_id: ws.userId,
        distance_m: payload.distance_m,
        speed_kmh: payload.speed_kmh,
        ts: payload.ts || Date.now(),
      });
      room.dirty = true;
      return;
    }

    // request_positions: reply to the requester only with the current buffer.
    if (event === 'request_positions') {
      send(ws, {
        event: 'gps_batch',
        payload: { racers: Array.from(room.gps.values()), ts: Date.now() },
      });
      return;
    }

    // Everything else that the room cares about is relayed to the others.
    if (FANOUT_EVENTS.has(event)) {
      broadcast(room, { event, payload }, ws);
      // A quit also drops the racer from the position buffer.
      if (event === 'racer_quit') room.gps.delete(payload.user_id || ws.userId);
      return;
    }

    // Unknown event — ignore.
  });

  function handleClose() {
    if (!room.clients.has(ws)) return; // already removed (e.g. replaced)
    room.clients.delete(ws);
    allClients.delete(ws);
    room.gps.delete(ws.userId);
    log(`LEAVE room=${roomId} user=${userId} role=${ws.role}`);
    // Mirror the app's racer_quit broadcast so peers drop the avatar.
    if (ws.role === 'racer') {
      broadcast(room, { event: 'racer_quit', payload: { user_id: ws.userId, ts: Date.now() } });
    }
    if (room.clients.size === 0) {
      rooms.delete(roomId);
      log(`ROOM CLOSED room=${roomId}`);
    }
  }

  ws.on('close', handleClose);
  ws.on('error', (e) => log('WS error', userId, e && e.message));
});

// ── GPS batching: one combined message per active room, every second ─────────
// Sent to ALL members (including senders) so everyone shares one authoritative
// snapshot. Skipped for rooms with no new updates since the last tick.
const batchTimer = setInterval(() => {
  for (const room of rooms.values()) {
    if (!room.dirty || room.gps.size === 0) continue;
    broadcast(room, {
      event: 'gps_batch',
      payload: { racers: Array.from(room.gps.values()), ts: Date.now() },
    });
    room.dirty = false;
  }
}, BATCH_INTERVAL_MS);

// ── Ping/pong: terminate connections that miss a heartbeat ───────────────────
const pingTimer = setInterval(() => {
  for (const ws of allClients) {
    if (ws.isAlive === false) {
      log('TERMINATE dead connection', ws.userId);
      ws.terminate(); // fires 'close' -> handleClose cleans up
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, PING_INTERVAL_MS);

server.listen(PORT, () => {
  log(`OneTraxx WS server listening on port ${PORT}`);
});

// ── Graceful shutdown (Render/Oracle send SIGTERM on redeploy/stop) ──────────
function shutdown(signal) {
  log(`${signal} received — shutting down.`);
  clearInterval(batchTimer);
  clearInterval(pingTimer);
  for (const ws of allClients) {
    try { ws.close(1001, 'server shutting down'); } catch (e) {}
  }
  wss.close(() => server.close(() => { log('Closed. Bye.'); process.exit(0); }));
  setTimeout(() => process.exit(0), 5000).unref(); // hard stop if not clean in 5s
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
