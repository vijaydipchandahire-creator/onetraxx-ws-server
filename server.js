'use strict';

require('dotenv').config();

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { URL } = require('url');
const { WebSocketServer, WebSocket } = require('ws');
const { createClient } = require('@supabase/supabase-js');

// Build/version metadata exposed on the health endpoint for the weekly
// primary/backup drift check (§10.5 proposal): src_sha256 fingerprints the
// exact deployed source, lock_sha256 the dependency lockfile (null when the
// file is absent). Computed once at boot; a mismatch between the two servers'
// values IS drift — the class of bug that has been caught manually 3 times.
const BUILD_INFO = (() => {
  const sha = (p) => {
    try { return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'); }
    catch (_) { return null; }
  };
  return {
    src_sha256: sha(__filename),
    lock_sha256: sha(path.join(__dirname, 'package-lock.json')),
    node: process.version,
    booted_at: new Date().toISOString(),
  };
})();

// @supabase/supabase-js builds a RealtimeClient on createClient(), which needs a
// global WebSocket. Node < 22 has none, so expose the one from `ws`. This server
// doesn't use Supabase Realtime (only token validation), but createClient throws
// without this on Node 20.
if (!globalThis.WebSocket) globalThis.WebSocket = WebSocket;

const PORT = parseInt(process.env.PORT, 10) || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// TEST_MODE: opt-in, env-gated auth bypass for LOCAL load/regression simulation
// only (scripts/simulate-race.js). When WS_TEST_MODE=1, the per-connection
// Supabase token validation is skipped so synthetic clients can connect. This is
// INERT in production — the env var is never set there — and the bypass is the
// only behavior it changes; routing, batching, caps and cleanup are untouched.
const TEST_MODE = process.env.WS_TEST_MODE === '1';

if (!TEST_MODE && (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY)) {
  console.error('[FATAL] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment.');
  process.exit(1);
}

// Service-role client, used only to validate access tokens on connect. Skipped
// entirely in TEST_MODE (no token validation happens there).
const supabase = TEST_MODE ? null : createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
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
const GPS_GRACE_MS      = 10000;  // keep a dropped racer's last position in the batch this long
const HOST_FINALIZE_GRACE_MS = GPS_GRACE_MS;  // reuse the existing lease window for "host genuinely gone"

// Terminal (race_over) delivery: re-send to each surviving socket until it
// acks or the socket dies. Bounded — never an infinite loop.
const TERMINAL_RETRY_MS     = 1200;
const TERMINAL_MAX_ATTEMPTS = 4;

// ── Live-race inactivity auto-abandon ────────────────────────────────────────
// Server-authoritative: a room accumulates "fully stationary" time only while
// NO racer reports movement. Any gps >= STATIONARY_KMH resets it. No tap/UI can
// reset it — only real GPS movement (mirrors the client's 0.222 m/s stall gate).
const RACE_INACTIVITY_MS      = 10 * 60 * 1000;  // stationary time before the warning
const RACE_INACTIVITY_WARN_MS = 60 * 1000;       // countdown shown before abandon
const INACTIVITY_TICK_MS      = 5000;            // sweep cadence
const STATIONARY_KMH          = 0.8;             // == 0.222 m/s (client stall gate)

// ── Per-racer eviction policy (2026-07-12, PROPOSAL_DNF_EVICTION_POLICY.md) ──
// REQ1: a racer whose transport is absent for 10 CONTINUOUS minutes is evicted
// (durable DNF via evict_racer_dnf). Any reconnect clears the clock, so
// screen-lock blips can never evict. REQ2: a CONNECTED racer stationary
// (<STATIONARY_KMH, judged on server-received gps frames) for 10 continuous
// minutes gets a 60s personal warning, then is evicted. Both paths re-read
// room_members.last_distance_m before evicting — progress there means the
// racer is alive on the Supabase fallback and the timer resets instead.
// SERVER_GONE_GRACE_MS mirrors the client's 90s "gone" window for the
// completion check ONLY (it never writes anything).
const RACER_GONE_EVICT_MS   = 10 * 60 * 1000;
const STATIONARY_EVICT_MS   = 10 * 60 * 1000;
const STATIONARY_WARN_MS    = 60 * 1000;
const SERVER_GONE_GRACE_MS  = 90 * 1000;
const COMPLETE_DEBOUNCE_MS  = 5000;              // one DB completion check per burst of events
const EVICT_PROGRESS_EPS_M  = 25;                // > half a client 50m save quantum = real progress
const EMPTY_ROOM_TTL_MS     = RACER_GONE_EVICT_MS + 5 * 60 * 1000; // keep empty active rooms for pending evictions

// Completion reasons the elected client driver only emits AFTER a DB-verified
// checkRaceComplete() (runAutoEnd / last-finisher / last-quitter). We treat these
// as authoritative "race is complete"; host_* reasons are host-initiated and need
// no server fallback (the host's own RPC call succeeds).
const VERIFIED_COMPLETE_REASONS = new Set(['all_done', 'all_finished']);

// roomId -> { clients: Set<ws>, gps: Map<userId, {user_id,distance_m,speed_kmh,ts}>, dirty: boolean }
const rooms = new Map();
const allClients = new Set();

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

function getRoom(roomId) {
  let room = rooms.get(roomId);
  if (!room) {
    room = { clients: new Set(), gps: new Map(), dirty: false,
             raceActive: false, lastMovementTs: 0, inactivityWarnDeadline: 0,
             // Eviction policy: per-racer lifecycle, keyed by userId.
             // { connected, disconnectedAt, lastMoveTs, lastDist, baselineDist,
             //   warnDeadline, finished, quit, evicted, evictReason,
             //   goneNotified, evicting, warnChecking }
             racers: new Map(),
             meta: undefined,          // { startedAt, targetM } cached on first eviction
             hasFinisher: false,       // monotonic: once true, room-level cancel is disabled
             completing: false, completeTimer: null,
             roomWarnChecking: false, emptySince: 0 };
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

// ── Reliable terminal delivery ────────────────────────────────────────────────
// race_over must actually reach every surviving socket; a single unacked send
// races the finisher's teardown. Pin the terminal payload on the room so a
// socket that (re)joins later still gets it, and re-send per client until acked.
function deliverTerminal(room, roomId, payload, exceptWs) {
  room.terminal = { payload, mid: `${roomId}:${Date.now()}` };
  log(`TERMINAL room=${roomId} reason=${payload && payload.reason}`);
  for (const client of room.clients) {
    if (client === exceptWs) continue;
    sendTerminalWithRetry(room, client);
  }
}

function sendTerminalWithRetry(room, client) {
  const terminal = room.terminal;
  if (!terminal) return;
  let attempts = 0;
  const attempt = () => {
    if (room.terminal !== terminal) return;                // superseded
    if (client.ackedTerminalMid === terminal.mid) return;  // delivered
    if (client.readyState !== WebSocket.OPEN) return;      // socket gone
    if (attempts++ >= TERMINAL_MAX_ATTEMPTS) return;       // bounded
    try { client.send(JSON.stringify({ event: 'race_over', payload: terminal.payload, mid: terminal.mid })); } catch (e) {}
    setTimeout(attempt, TERMINAL_RETRY_MS);
  };
  attempt();
}

// Finalize a room's status server-side ONLY as a fallback for a genuinely-absent
// host. Completeness is NOT decided here — it is inherited from the caller's
// verified race_over reason (checkRaceComplete already passed on a client). This
// function only (a) confirms the host is truly gone via WS liveness + grace, and
// (b) performs the guarded DB write. Idempotent and safe to call repeatedly.
async function maybeServerFinalize(roomId, room) {
  if (!supabase) return;             // TEST_MODE: no DB client — path is fully inert
  if (room.finalizing) return;       // one in-flight attempt per room

  // Resolve the host authoritatively, once (never trust a client-supplied id).
  if (room.hostId === undefined) {
    try {
      const { data } = await supabase
        .from('race_rooms').select('host_id, status').eq('id', roomId).single();
      room.hostId = data ? data.host_id : null;
      if (data && data.status !== 'racing') return;   // already settled/closed
    } catch (e) { log('SERVER-FINALIZE host lookup failed', roomId, e && e.message); return; }
  }
  if (!room.hostId) return;

  const hostConnected = () => {
    for (const c of room.clients) if (c.userId === room.hostId) return true;
    return false;
  };
  if (hostConnected()) return;       // host present → its own client finalizes via RPC

  room.finalizing = true;
  // Lease/grace: let the host reconnect (screen-lock / blip) within the existing
  // window before we finalize on their behalf. Mirrors GPS_GRACE_MS semantics.
  setTimeout(async () => {
    const r = rooms.get(roomId);
    if (r === room && hostConnected()) { room.finalizing = false; return; }  // host came back → stand down
    // Room object REPLACED (a rejoin rebuilt it) → the new room's own lifecycle
    // owns settlement; stand down. But room GONE (r === undefined) must NOT
    // stand down: when every survivor drops their socket right after the
    // terminal, teardown (rooms.delete + ROOM CLOSED) used to abort this timer
    // and the status write was lost — the room sat 'racing' forever and Browse
    // kept offering Spectate on a dead race (BADGE2 2026-07-13). The CAS below
    // is DB-guarded, so running it after teardown is safe.
    if (r !== room && r !== undefined) { room.finalizing = false; return; }
    try {
      // Guarded to status='racing' so it can NEVER override a finished/cancelled
      // room, and never finalizes a race that isn't actually running.
      const { error } = await supabase
        .from('race_rooms').update({ status: 'finished' })
        .eq('id', roomId).eq('status', 'racing');
      if (error) { room.finalizing = false; log('SERVER-FINALIZE failed', roomId, error.message); }
      else {
        log(`SERVER-FINALIZE room=${roomId} host=${room.hostId} (absent; race complete${r === room ? '' : '; room already closed'})`);
        // The DB write alone ends nothing on screen — push the terminal state
        // to every surviving socket too (GPS-grace path). Skipped when the room
        // is already closed: there is no socket left to deliver to.
        if (r === room) deliverTerminal(room, roomId, { reason: 'all_done', server_finalized: true });
      }
    } catch (e) { room.finalizing = false; log('SERVER-FINALIZE error', roomId, e && e.message); }
  }, HOST_FINALIZE_GRACE_MS);
}

// Auto-abandon a race that has been fully stationary too long. Sets status to
// 'cancelled' (NOT 'finished') and pushes a distinct terminal reason. Writes NO
// per-user stats — clients short-circuit their stats path on this reason.
// Deliberately NOT in VERIFIED_COMPLETE_REASONS (that path forces 'finished').
async function abandonRaceInactive(roomId, room) {
  if (room.terminal) return;                 // already ending
  log(`INACTIVITY-ABANDON room=${roomId}`);
  if (supabase) {
    try {
      await supabase.from('race_rooms').update({ status: 'cancelled' })
        .eq('id', roomId).eq('status', 'racing');   // guarded: never overrides a settled room
    } catch (e) { log('ABANDON status update failed', roomId, e && e.message); }
  }
  deliverTerminal(room, roomId, { reason: 'inactivity_abandon' });
}

// ── Eviction policy (2026-07-12) ─────────────────────────────────────────────
// Evict one racer: durable DNF via the service-role-only evict_racer_dnf RPC
// (no stats, membership row retained, advisory-locked, stands down on a settled
// room or an existing finish). Guarded by a DB last_distance_m re-read so a
// racer alive on the Supabase fallback is never evicted — their timer resets.
async function evictRacer(roomId, room, userId, reason) {
  if (!supabase || room.terminal) return;                 // TEST_MODE: fully inert
  const st = room.racers.get(userId);
  if (!st || st.evicted || st.evicting || st.finished || st.quit) return;
  st.evicting = true;
  try {
    const { data: m, error: mErr } = await supabase.from('room_members')
      .select('last_distance_m').eq('room_id', roomId).eq('user_id', userId)
      .eq('role', 'racer').maybeSingle();
    if (mErr) { log('EVICT member read failed', roomId, userId, mErr.message); return; }
    if (!m) { st.quit = true; scheduleCompletionCheck(roomId, room); return; } // row gone → already quit/kicked
    const dbDist = m.last_distance_m || 0;
    const baseline = Math.max(st.baselineDist || 0, st.lastDist || 0);
    if (dbDist > baseline + EVICT_PROGRESS_EPS_M) {
      // Fallback-transport liveness: distance advanced without the WS seeing it.
      st.lastDist = dbDist; st.baselineDist = dbDist;
      st.lastMoveTs = Date.now(); st.warnDeadline = 0;
      if (st.disconnectedAt) st.disconnectedAt = Date.now();  // still gone from WS: restart the 10min clock
      log(`EVICT stand-down (fallback progress) room=${roomId} user=${userId} db=${dbDist}`);
      return;
    }
    if (!room.meta) {
      const { data: r } = await supabase.from('race_rooms')
        .select('started_at, target_distance_m, status').eq('id', roomId).single();
      if (!r || r.status !== 'racing') return;              // settled — terminal flow owns everyone
      room.meta = { startedAt: r.started_at, targetM: r.target_distance_m };
    }
    const timeMs = room.meta.startedAt
      ? Math.max(0, Date.now() - new Date(room.meta.startedAt).getTime()) : 0;
    const dist = Math.max(dbDist, room.gps.get(userId)?.distance_m || 0, st.lastDist || 0);
    const { data: verdict, error } = await supabase.rpc('evict_racer_dnf', {
      p_room_id: roomId, p_user_id: userId, p_distance_m: Math.round(dist),
      p_time_ms: timeMs, p_reason: reason,
    });
    if (error) { log('EVICT rpc failed', roomId, userId, error.message); return; }
    if (verdict === 'room_settled' || verdict === 'no_room') return;
    if (verdict === 'already_finished') { st.finished = true; room.hasFinisher = true; return; }
    st.evicted = true; st.evictReason = reason;
    room.gps.delete(userId);
    log(`EVICT room=${roomId} user=${userId} reason=${reason} dist=${Math.round(dist)} time=${timeMs}`);
    // Peers render this exactly like a quit (✕ + completion math via the DNF row);
    // the victim gets a targeted terminal instead (now, or on reconnect via JOIN).
    let victimWs = null;
    for (const c of room.clients) if (c.userId === userId) victimWs = c;
    broadcast(room, { event: 'racer_quit', payload: { user_id: userId, evicted: true } }, victimWs);
    if (victimWs) send(victimWs, { event: 'evicted', payload: { user_id: userId, reason } });
    scheduleCompletionCheck(roomId, room);
  } catch (e) { log('EVICT error', roomId, userId, e && e.message); }
  finally { st.evicting = false; }
}

// REQ2 warning, guarded (C5): never start an eviction countdown for the LAST
// active racer of a zero-finisher room — the room-level cancel owns that case,
// so an all-DNF 'finished' race is unreachable. Also captures the DB-progress
// baseline the eviction-time liveness re-check compares against.
async function maybeWarnStationary(roomId, room, userId, st) {
  if (!supabase || st.warnChecking) return;               // TEST_MODE: inert
  st.warnChecking = true;
  try {
    const [{ data: members }, { data: results }] = await Promise.all([
      supabase.from('room_members').select('user_id, last_distance_m')
        .eq('room_id', roomId).eq('role', 'racer'),
      supabase.from('race_results').select('user_id, finish_position').eq('room_id', roomId),
    ]);
    const hasFinisher = (results || []).some(r => r.finish_position != null);
    if (hasFinisher) room.hasFinisher = true;
    const resolved = new Set((results || []).map(r => r.user_id));
    const others = (members || []).filter(mm => mm.user_id !== userId && !resolved.has(mm.user_id)).length;
    if (!hasFinisher && others === 0) return;             // stand down: room-level backstop owns it
    if (room.terminal || room.inactivityWarnDeadline) return;
    if (st.finished || st.quit || st.evicted || st.warnDeadline) return;
    if (Date.now() - st.lastMoveTs < STATIONARY_EVICT_MS) return;  // moved during the check
    const myRow = (members || []).find(mm => mm.user_id === userId);
    st.baselineDist = Math.max(myRow?.last_distance_m || 0, st.lastDist || 0);
    st.warnDeadline = Date.now() + STATIONARY_WARN_MS;
    for (const c of room.clients) if (c.userId === userId)
      send(c, { event: 'stationary_warning', payload: { deadline: st.warnDeadline } });
    log(`STATIONARY-WARN room=${roomId} user=${userId}`);
  } catch (e) { log('STATIONARY-WARN error', roomId, userId, e && e.message); }
  finally { st.warnChecking = false; }
}

// Debounced server-side completion check. Exists because finishers may leave
// the HUD (REQ3) — with no client left to run checkRaceComplete, the server
// must settle the room. Semantics mirror the client: complete when every
// roster racer has a race_results row OR was WITNESSED disconnected >90s by
// THIS server (its own transport record, never presence).
function scheduleCompletionCheck(roomId, room) {
  if (!supabase || room.terminal || room.completeTimer) return;
  room.completeTimer = setTimeout(() => {
    room.completeTimer = null;
    maybeCompleteRace(roomId, room);
  }, COMPLETE_DEBOUNCE_MS);
}

async function maybeCompleteRace(roomId, room) {
  if (!supabase || room.terminal || room.completing) return;
  room.completing = true;
  try {
    const [{ data: members, error: mErr }, { data: results, error: rErr }] = await Promise.all([
      supabase.from('room_members').select('user_id').eq('room_id', roomId).eq('role', 'racer'),
      supabase.from('race_results').select('user_id, finish_position').eq('room_id', roomId),
    ]);
    if (mErr || rErr || !members || members.length === 0) return;
    const settled = new Set((results || []).map(r => r.user_id));
    const hasFinisher = (results || []).some(r => r.finish_position != null);
    const now = Date.now();
    const unresolved = members.filter(({ user_id }) => {
      if (settled.has(user_id)) return false;
      // The 90s-gone shortcut only applies when a FINISHER is waiting on the
      // straggler (client-parity: only a present finisher/driver ever completed
      // on the gone window). Without one, nothing but a durable result row
      // (quit/eviction) resolves a racer — a zero-finisher room can never be
      // settled out from under a screen-locked racer who is still racing.
      if (!hasFinisher) return true;
      const st = room.racers.get(user_id);
      return !(st && !st.connected && st.disconnectedAt && now - st.disconnectedAt >= SERVER_GONE_GRACE_MS);
    });
    if (unresolved.length) return;
    if (hasFinisher) {
      // Status CAS (same guard as maybeServerFinalize): exactly one settler wins;
      // deliver the terminal only if WE won, so reasons never conflict.
      const { data: won, error } = await supabase.from('race_rooms')
        .update({ status: 'finished' }).eq('id', roomId).eq('status', 'racing').select('id');
      if (error) { log('SERVER-COMPLETE failed', roomId, error.message); return; }
      if (won && won.length) {
        log(`SERVER-COMPLETE room=${roomId}`);
        deliverTerminal(room, roomId, { reason: 'all_done', server_finalized: true });
      }
    } else {
      // C5: zero finishers → a race with no outcome is 'cancelled', never an
      // all-DNF 'finished'.
      const { data: won, error } = await supabase.from('race_rooms')
        .update({ status: 'cancelled' }).eq('id', roomId).eq('status', 'racing').select('id');
      if (error) { log('SERVER-COMPLETE cancel failed', roomId, error.message); return; }
      if (won && won.length) {
        log(`SERVER-COMPLETE (no finishers → cancelled) room=${roomId}`);
        deliverTerminal(room, roomId, { reason: 'inactivity_abandon' });
      }
    }
  } catch (e) { log('SERVER-COMPLETE error', roomId, e && e.message); }
  finally { room.completing = false; }
}
// ── end eviction policy functions ────────────────────────────────────────────

// Close a room the HOST explicitly quit but whose own client failed to finalize
// (e.g. a ghost-inflated remaining-racer count left the room 'racing' with a
// spectator present — the QA6 zombie-room bug). Authoritative + immune to client
// state: only proceeds when the host is genuinely gone (grace lets a quit→rejoin
// stand down), counts REAL remaining racers from the DB (role='racer' members,
// excl the host; quit racers already deleted their row, screen-locked racers keep
// theirs so they are not miscounted as gone), and preserves the product rule that
// a host-less race CONTINUES while >=2 racers remain. Idempotent; guarded write
// can never override a settled room.
async function maybeCloseHostAbandoned(roomId, room) {
  if (!supabase) return;                        // TEST_MODE: inert
  if (room.terminal || room.hostClosing) return;
  if (room.hostId === undefined) {
    try {
      const { data } = await supabase.from('race_rooms').select('host_id, status').eq('id', roomId).single();
      room.hostId = data ? data.host_id : null;
      if (data && data.status !== 'racing') return;   // already settled/closed
    } catch (e) { log('HOST-ABANDON host lookup failed', roomId, e && e.message); return; }
  }
  if (!room.hostId) return;
  const hostConnected = () => {
    for (const c of room.clients) if (c.userId === room.hostId) return true;
    return false;
  };
  if (hostConnected()) return;                  // host still here (quit→rejoin, or a non-host quit)
  room.hostClosing = true;
  setTimeout(async () => {
    const r = rooms.get(roomId);
    if (r !== room || room.terminal) { room.hostClosing = false; return; }
    if (hostConnected()) { room.hostClosing = false; return; }   // host returned within grace
    try {
      const { count, error: cErr } = await supabase.from('room_members')
        .select('user_id', { count: 'exact', head: true })
        .eq('room_id', roomId).eq('role', 'racer').neq('user_id', room.hostId);
      if (cErr) { room.hostClosing = false; log('HOST-ABANDON count failed', roomId, cErr.message); return; }
      if ((count || 0) > 1) { room.hostClosing = false; log(`HOST-ABANDON stand-down room=${roomId} remaining=${count}`); return; }
      const { error } = await supabase.from('race_rooms').update({ status: 'cancelled' })
        .eq('id', roomId).eq('status', 'racing');
      if (error) { room.hostClosing = false; log('HOST-ABANDON cancel failed', roomId, error.message); return; }
      log(`HOST-ABANDON-CLOSE room=${roomId} host=${room.hostId} remaining=${count}`);
      deliverTerminal(room, roomId, { reason: 'host_quit' });
    } catch (e) { room.hostClosing = false; log('HOST-ABANDON error', roomId, e && e.message); }
  }, HOST_FINALIZE_GRACE_MS);
}

// Host-only racer kick. Verifies the sender is the room host (reusing the cached
// room.hostId), drops the target from the GPS fan-out, tells the room (incl. the
// target) via racer_kicked, and best-effort closes the target's socket. The DB
// room_members delete + LiveKit removal are done by the host client / Edge Function.
async function handleKickRacer(ws, room, roomId, payload) {
  const targetId = payload && payload.user_id;
  if (!targetId || !supabase) return;              // inert in TEST_MODE
  if (room.hostId === undefined) {
    try {
      const { data } = await supabase.from('race_rooms').select('host_id').eq('id', roomId).single();
      room.hostId = data ? data.host_id : null;
    } catch (e) { log('KICK host lookup failed', roomId, e && e.message); return; }
  }
  if (ws.userId !== room.hostId) { log('KICK rejected (non-host)', { roomId, from: ws.userId }); return; }
  room.gps.delete(targetId);                        // stop including target in the next batch
  broadcast(room, { event: 'racer_kicked', payload: { user_id: targetId } });   // to all, incl. target
  for (const c of room.clients) {                   // best-effort force-close the target socket
    if (c.userId === targetId) { try { c.close(CLOSE_BAD_REQUEST, 'kicked by host'); } catch (_) {} }
  }
  log(`KICK room=${roomId} target=${targetId} by host=${ws.userId}`);
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
    res.end(req.method === 'HEAD' ? undefined : JSON.stringify({ status: 'ok', rooms: rooms.size, connections: allClients.size, ...BUILD_INFO }));
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
  // Skipped in TEST_MODE (local simulation only) — synthetic clients have no
  // real Supabase JWT.
  if (!TEST_MODE) {
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
  room.emptySince = 0;

  // Eviction policy: (re)seed this racer's lifecycle state. A reconnect within
  // the 10-min window clears the disconnect clock — full restore, exactly as
  // before. An already-evicted racer gets the pinned 'evicted' terminal instead
  // of silently re-entering the live race.
  if (role === 'racer') {
    let st = room.racers.get(userId);
    if (!st) {
      st = { connected: true, disconnectedAt: 0, lastMoveTs: Date.now(), lastDist: 0,
             baselineDist: 0, warnDeadline: 0, finished: false, quit: false,
             evicted: false, goneNotified: false, evicting: false, warnChecking: false };
      room.racers.set(userId, st);
    }
    st.connected = true; st.disconnectedAt = 0; st.goneNotified = false;
    st.lastMoveTs = Date.now();                 // reconnect re-seeds the stationary clock
    if (st.evicted) send(ws, { event: 'evicted', payload: { user_id: userId, reason: st.evictReason || 'disconnect_timeout' } });
  }

  // A socket (re)joining a room that already ended must learn that immediately —
  // the original terminal broadcast predates this connection.
  if (room.terminal) sendTerminalWithRetry(room, ws);

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

    // Delivery receipt for enveloped sends: any message carrying a mid gets an
    // ack back to the SENDER so its bounded retry loop can stop.
    if (typeof msg.mid === 'string') send(ws, { event: 'ack', payload: { mid: msg.mid } });

    // Client confirms it received the terminal broadcast — stop re-sending.
    if (event === 'race_over_ack') {
      ws.ackedTerminalMid = payload.mid;
      return;
    }

    // App-level keepalive: echo pings so the client can measure TRANSPORT
    // liveness even when the room is quiet (no gps_batch traffic). Before
    // this, 'ping' fell through to "unknown event — ignore", so a silent
    // room was indistinguishable from a dead socket (ISS-03).
    if (event === 'ping') {
      send(ws, { event: 'pong', payload: { ts: payload.ts || Date.now() } });
      return;
    }

    // gps: buffer the racer's latest position; the batch timer fans it out.
    if (event === 'gps') {
      if (ws.role !== 'racer') return; // only racers contribute positions
      const st = room.racers.get(ws.userId);
      if (st && st.evicted) return;    // evicted racers no longer feed the room
      room.gps.set(ws.userId, {
        user_id: ws.userId,
        distance_m: payload.distance_m,
        speed_kmh: payload.speed_kmh,
        ts: payload.ts || Date.now(),
      });
      room.dirty = true;
      // First gps marks the race live and seeds the movement clock.
      if (!room.raceActive) { room.raceActive = true; room.lastMovementTs = Date.now(); }
      // Real movement resets the stationary clock; if a warning is showing, cancel
      // it — movement is the ONLY thing that can clear the abandon countdown.
      if (typeof payload.speed_kmh === 'number' && payload.speed_kmh >= STATIONARY_KMH) {
        room.lastMovementTs = Date.now();
        if (room.inactivityWarnDeadline) {
          room.inactivityWarnDeadline = 0;
          broadcast(room, { event: 'inactivity_cleared', payload: {} });
        }
      }
      // Eviction policy (REQ2): per-racer movement, judged on the frames THIS
      // server receives — speed at/above the stall gate OR the accumulated
      // distance advancing (so a forged speed field alone is not the signal).
      if (st) {
        const d = typeof payload.distance_m === 'number' ? payload.distance_m : 0;
        const moved = (typeof payload.speed_kmh === 'number' && payload.speed_kmh >= STATIONARY_KMH)
                      || d > (st.lastDist || 0) + 2;
        if (d > (st.lastDist || 0)) st.lastDist = d;
        if (moved) {
          st.lastMoveTs = Date.now();
          if (st.warnDeadline) {
            st.warnDeadline = 0;
            send(ws, { event: 'stationary_cleared', payload: {} });
          }
        }
      }
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
      // Terminal event: reliable fan-out (ack + bounded retry) instead of a
      // fire-and-forget relay — the sender navigates away and drops its socket
      // immediately, but survivors must still end their HUD.
      // Server-authoritative finalize FALLBACK is unchanged, just relocated:
      // when the client driver declares a DB-verified completion but the host
      // is gone, the driver's update_room_status RPC is rejected (host-only).
      // Persist the status the client couldn't.
      if (event === 'race_over') {
        // Only the HOST may terminate the room for everyone — UNLESS the reason is
        // a DB-verified completion (all_done/all_finished) that any elected client
        // driver may legitimately report. Blocks a non-host racer/spectator from
        // griefing the room with a forged race_over (H7).
        const verified = VERIFIED_COMPLETE_REASONS.has(payload.reason);
        (async () => {
          if (!verified) {
            if (room.hostId === undefined && supabase) {
              try {
                const { data } = await supabase.from('race_rooms').select('host_id').eq('id', roomId).single();
                room.hostId = data ? data.host_id : null;
              } catch (e) { if (room.hostId === undefined) room.hostId = null; }
            }
            if (!room.hostId || ws.userId !== room.hostId) {
              log(`RACE_OVER rejected (non-host, unverified) room=${roomId} from=${ws.userId} reason=${payload && payload.reason}`);
              return;
            }
          }
          deliverTerminal(room, roomId, payload, ws);
          if (verified) maybeServerFinalize(roomId, room);
        })();
        return;
      }
      // Never trust a client-supplied identity on a relayed event — a racer can
      // only quit/finish as THEMSELVES. Overwrite with the authenticated id before
      // fan-out so a forged payload.user_id can't eject/forge a peer (H7).
      if (event === 'racer_quit' || event === 'finished') payload.user_id = ws.userId;
      // Eviction policy: track per-racer terminal state (exempts finishers from
      // the stationary sweep) and re-check completion — the DNF/finish row this
      // event implies may be the last unresolved racer (REQ3: finishers may
      // have left the HUD, so the server must be able to settle).
      if (event === 'finished' || event === 'racer_quit') {
        const st = room.racers.get(ws.userId);
        if (st) { if (event === 'finished') st.finished = true; else st.quit = true; }
        if (event === 'finished') room.hasFinisher = true;
        scheduleCompletionCheck(roomId, room);
      }
      broadcast(room, { event, payload }, ws);
      // A quit also drops the racer from the position buffer.
      if (event === 'racer_quit') {
        room.gps.delete(ws.userId);
        // If the HOST is the one who quit, the room may now be host-less. Close it
        // (graced, guarded, DB-authoritative racer count) as a safety net for when
        // the host's own client failed to finalize. Self-gates: stands down at once
        // if the host is still connected (a normal racer's quit, or host quit→rejoin).
        maybeCloseHostAbandoned(roomId, room);
      }
      return;
    }

    // Host-only: kick a racer. Verified server-side against race_rooms.host_id —
    // never relayed blindly (not in FANOUT_EVENTS).
    if (event === 'kick_racer') {
      handleKickRacer(ws, room, roomId, payload);
      return;
    }

    // Unknown event — ignore.
  });

  function handleClose(code, reason) {
    if (!room.clients.has(ws)) return; // already removed (e.g. replaced) — never broadcast
    room.clients.delete(ws);
    allClients.delete(ws);
    // Do NOT drop the racer's last position immediately. A screen-lock / network
    // blip / socket recycle closes the transport while the racer is still racing
    // (GPS continues via FGS, they reconnect within seconds). Removing them from
    // room.gps here blanks their avatar out of the batch for every other viewer
    // until they reconnect. Keep the last snapshot for a short grace window; if no
    // connection for this user exists when it elapses, then drop it. (An explicit
    // racer_quit still deletes room.gps immediately via the FANOUT path.)
    const closingUserId = ws.userId;
    setTimeout(() => {
      const r = rooms.get(roomId);
      if (!r || r !== room) return;                                  // room torn down/replaced
      for (const c of r.clients) if (c.userId === closingUserId) return; // reconnected — keep
      r.gps.delete(closingUserId);
    }, GPS_GRACE_MS);
    // code 1000/1001 = deliberate client close; 1006/undefined = abnormal drop
    // (network, proxy, or our TERMINATE). Makes the next flap report diagnosable.
    log(`LEAVE room=${roomId} user=${userId} role=${ws.role} code=${code ?? 'n/a'}${reason && reason.length ? ` reason=${reason.toString()}` : ''}`);
    // NOTE: do NOT synthesize racer_quit on transport close. A racer who locks
    // their screen / hits a network blip / gets their socket recycled is still
    // racing (GPS continues via FGS, positions flow over Supabase). The app
    // sends an explicit racer_quit on a real quit (relayed via FANOUT_EVENTS).
    // Peers detect genuine departures via presence + stall/frozen UI.
    // Auto-broadcasting here caused opponents to show a false X on screen lock.
    // Eviction policy (REQ1): start the racer's continuous-absence clock. Any
    // reconnect (JOIN) clears it — this only ever ages if they truly stay gone.
    if (ws.role === 'racer') {
      const st = room.racers.get(closingUserId);
      if (st && !st.finished && !st.quit && !st.evicted) {
        st.connected = false;
        st.disconnectedAt = Date.now();
        st.baselineDist = Math.max(st.baselineDist || 0, st.lastDist || 0);
      }
    }
    if (room.clients.size === 0) {
      // Keep an active race's room alive so pending disconnect-eviction timers
      // can still fire; the lifecycle sweep GCs it once settled or aged out.
      if (room.raceActive && !room.terminal) {
        room.emptySince = Date.now();
        log(`ROOM EMPTY (kept for eviction timers) room=${roomId}`);
      } else {
        rooms.delete(roomId);
        log(`ROOM CLOSED room=${roomId}`);
      }
    }
  }

  ws.on('close', (code, reason) => handleClose(code, reason));
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

// ── Racer lifecycle sweep (extends the old inactivity sweep) ─────────────────
// Ordering per room, per tick, IS the timer-precedence rule:
//   1. GC empty rooms kept alive for eviction timers, once settled/aged out.
//   2. ROOM-LEVEL backstop (re-scoped): warn+cancel only while NO racer has
//      finished. With >=1 finisher the room must end 'finished' via per-racer
//      eviction + completion — never a room-wide cancel that hides saved
//      results (REQ4 root cause / stationary-H).
//   3. PER-RACER (suspended while a room-level warning is counting down, so
//      room-cancel always beats individual eviction when they coincide):
//      REQ1 disconnect eviction, 90s-gone completion aging, REQ2 stationary
//      warn/evict.
function roomLevelSweep(roomId, room, now) {
  if (room.hasFinisher) return;                     // monotonic: finishers disable room-cancel
  if (countByRole(room, 'racer') === 0) return;
  const stationaryFor = now - room.lastMovementTs;
  if (!room.inactivityWarnDeadline) {
    if (stationaryFor >= RACE_INACTIVITY_MS && !room.roomWarnChecking) {
      room.roomWarnChecking = true;
      (async () => {
        try {
          // Finisher check is DB-authoritative once, at warn time — a finish that
          // happened while this server wasn't watching (Supabase fallback) still
          // disables the room-level cancel.
          let finishers = 0;
          for (const st of room.racers.values()) if (st.finished) finishers++;
          if (!finishers && supabase) {
            const { count } = await supabase.from('race_results')
              .select('user_id', { count: 'exact', head: true })
              .eq('room_id', roomId).not('finish_position', 'is', null);
            finishers = count || 0;
          }
          if (finishers > 0) { room.hasFinisher = true; return; }
          if (room.terminal || room.inactivityWarnDeadline) return;
          if (Date.now() - room.lastMovementTs < RACE_INACTIVITY_MS) return;  // moved during the check
          room.inactivityWarnDeadline = Date.now() + RACE_INACTIVITY_WARN_MS;
          broadcast(room, { event: 'inactivity_warning', payload: { deadline: room.inactivityWarnDeadline } });
          log(`INACTIVITY-WARN room=${roomId}`);
        } catch (e) { log('INACTIVITY-WARN check error', roomId, e && e.message); }
        finally { room.roomWarnChecking = false; }
      })();
    }
  } else if (now >= room.inactivityWarnDeadline) {
    room.inactivityWarnDeadline = 0;
    abandonRaceInactive(roomId, room);
  }
}

function perRacerSweep(roomId, room, now) {
  for (const [uid, st] of room.racers) {
    if (st.finished || st.quit || st.evicted || st.evicting) continue;
    // REQ1: 10 min of continuous transport absence → evict. No warning exists
    // on this path by design — there is no socket to warn (C2).
    if (st.disconnectedAt && now - st.disconnectedAt >= RACER_GONE_EVICT_MS) {
      evictRacer(roomId, room, uid, 'disconnect_timeout');
      continue;
    }
    // 90s-gone aging feeds ONLY the completion check; it writes nothing.
    if (st.disconnectedAt && !st.goneNotified && now - st.disconnectedAt >= SERVER_GONE_GRACE_MS) {
      st.goneNotified = true;
      scheduleCompletionCheck(roomId, room);
    }
    // REQ2: stationary while connected (a disconnected racer is governed by
    // the REQ1 clock instead — the two timers are mutually exclusive).
    if (st.connected) {
      if (!st.warnDeadline && now - st.lastMoveTs >= STATIONARY_EVICT_MS) {
        maybeWarnStationary(roomId, room, uid, st);
      } else if (st.warnDeadline && now >= st.warnDeadline) {
        evictRacer(roomId, room, uid, 'stationary_timeout');
      }
    }
  }
}

const inactivityTimer = setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of rooms) {
    if (room.clients.size === 0 && room.emptySince &&
        (room.terminal || now - room.emptySince > EMPTY_ROOM_TTL_MS)) {
      if (room.completeTimer) clearTimeout(room.completeTimer);
      rooms.delete(roomId);
      log(`ROOM GC room=${roomId}`);
      continue;
    }
    if (!room.raceActive || room.terminal) continue;
    roomLevelSweep(roomId, room, now);
    if (!room.inactivityWarnDeadline) perRacerSweep(roomId, room, now);
  }
}, INACTIVITY_TICK_MS);

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
  if (TEST_MODE) log('⚠️  WS_TEST_MODE=1 — AUTH BYPASSED. Local simulation only; never run this in production.');
});

// ── Graceful shutdown (Render/Oracle send SIGTERM on redeploy/stop) ──────────
function shutdown(signal) {
  log(`${signal} received — shutting down.`);
  clearInterval(batchTimer);
  clearInterval(pingTimer);
  clearInterval(inactivityTimer);
  for (const ws of allClients) {
    try { ws.close(1001, 'server shutting down'); } catch (e) {}
  }
  wss.close(() => server.close(() => { log('Closed. Bye.'); process.exit(0); }));
  setTimeout(() => process.exit(0), 5000).unref(); // hard stop if not clean in 5s
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
