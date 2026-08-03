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

// ── D-08 (2026-08-03): optional server-side Sentry ───────────────────────────
// THE SOFT REQUIRE IS THE WHOLE DESIGN — do not "clean it up" into a normal
// top-level require. deploy.sh ships server.js and NOTHING ELSE; it has never
// installed dependencies, and its step-4 guard is `node --check`, a SYNTAX check
// that does not resolve require(). A hard require for a module the VM has not
// installed would therefore pass every gate in the deploy script and then crash
// the process at boot, leaving only the `pm2 jlist` online-grep between a deploy
// and a dead relay. The two ends also install differently and can legitimately be
// out of step: the Oracle VM is hand-run `npm install` on Node 20, ws-backup is
// Render `buildCommand: npm install` on Node 26.
//
// With the soft require, DEPLOY ORDER STOPS MATTERING and either end may run
// without the module — it degrades to exactly today's behaviour (PM2 logs only).
// That is what lets this land and deploy BEFORE the dependency exists anywhere.
//
// Gated on SENTRY_DSN as well, so an environment without a DSN — TEST_MODE,
// a fresh box, a local run — is fully inert rather than merely quiet.
//
// The release is the source fingerprint the health endpoint already exposes, so
// a Sentry event names the exact deployed file and lines up with the drift check.
let Sentry = null;
try {
  if (process.env.SENTRY_DSN) {
    Sentry = require('@sentry/node');
    Sentry.init({
      dsn: process.env.SENTRY_DSN,
      environment: process.env.SENTRY_ENV || 'production',
      release: BUILD_INFO.src_sha256 ? `ws@${BUILD_INFO.src_sha256.slice(0, 12)}` : undefined,
      // Errors only. This process is a hot relay on a free-tier-adjacent box;
      // performance tracing would add per-message overhead for no current need.
      tracesSampleRate: 0,
    });
  }
} catch (e) {
  // Module absent, or init threw. Either way the server keeps running exactly as
  // it does today — this must never be the reason a race server fails to boot.
  console.error('[sentry] disabled:', e && e.message);
  Sentry = null;
}

// Never throws, never blocks. Same contract as markLifecycle: a failure here
// degrades to "no event was sent", which is the pre-D-08 state.
function captureServer(err, ctx) {
  if (!Sentry) return;
  try {
    Sentry.captureException(err instanceof Error ? err : new Error(String(err)),
      ctx ? { extra: ctx } : undefined);
  } catch (_) {}
}

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
//
// ⚠️ THAT STAND-DOWN IS MUCH WEAKER THAN THE LINE ABOVE READS (measured E-13,
// 2026-08-03). room_members.last_distance_m has exactly ONE writer in the whole
// client — the AppState->'background' handler at LiveRaceScreen.js:519. There is
// no periodic write. handleClose seeds st.baselineDist to the last WS-known
// distance, so the stand-down fires only if a FURTHER background transition
// happens during the outage. A racer in a tunnel with the app in the FOREGROUND
// can never be protected by it, and neither can one already backgrounded and
// staying there. Do not cite it as general protection for live racers.
//
// SERVER_GONE_GRACE_MS mirrors the client's 90s "gone" window for the
// completion check ONLY (it never writes anything).

// E-13: 3min -> 10min, 2026-08-03, reverting the 2026-07-20 retune with the
// owner's explicit go. The 07-20 change to 3min was made to clear GHOST AVATARS
// (a swipe-killed racer's frozen avatar persists in peers' standings until the
// eviction ✕), and it leaned on the fallback-progress stand-down to protect real
// racers — which the note above shows it largely does not. So the cutoff was
// buying a UI cleanup at the price of durable false DNFs on anyone who loses
// signal for 3 minutes.
//
// 10min is a RISK REDUCTION, NOT THE TARGET MODEL. Under the target model socket
// loss must never affect membership at all; that needs the eviction split into an
// ephemeral presence signal (peers' standings) plus a durable mark at room close,
// which is a client+server change requiring an OTA and 12/12 adoption gating —
// removing the DNF row pre-adoption collapses checkRaceComplete's goneCount to 0
// and freezes every un-adopted client's room permanently. Tracked separately.
//
// EMPTY_ROOM_TTL_MS self-derives from this and follows to 15min.
const RACER_GONE_EVICT_MS   = 10 * 60 * 1000;
const STATIONARY_EVICT_MS   = 10 * 60 * 1000;
const STATIONARY_WARN_MS    = 60 * 1000;
const SERVER_GONE_GRACE_MS  = 90 * 1000;
const COMPLETE_DEBOUNCE_MS  = 5000;              // one DB completion check per burst of events
// A finisher whose race_results write failed has no row, so `settled` in
// maybeCompleteRace never sees them; they are still connected, so the 90s-gone
// shortcut cannot resolve them either, and the room hangs until the stationary sweep
// DNFs a racer who genuinely finished. This resolves them from the server's OWN
// witnessed 'finished' relay (st.finished, set only after payload.user_id is
// overwritten with the authenticated id, so it can never be forged for a peer).
// DELIBERATELY longer than the client's FINISH_NOROW_DWELL_MS: the client polls every
// 10s and must always be the one to settle when any driver is present, so this term
// can only ever fire in a room the clients have already left or cannot settle.
// RETUNED 30s -> 90s to track the client's 15s -> 60s (c9eb5d7). The client's worst
// case is dwell 60s + up to one 10s heartbeat = 70s, and 90s clears that by 20s, plus
// COMPLETE_DEBOUNCE_MS (5s) on top of the trigger. Any future change to the client
// constant MUST raise this in the same batch or the ordering guarantee is lost.
const FINISH_NOROW_DWELL_MS = 90 * 1000;
const EVICT_PROGRESS_EPS_M  = 25;                // > half a client 50m save quantum = real progress
const EMPTY_ROOM_TTL_MS     = RACER_GONE_EVICT_MS + 5 * 60 * 1000; // keep empty active rooms for pending evictions

// Completion reasons the elected client driver only emits AFTER a DB-verified
// checkRaceComplete() (runAutoEnd / last-finisher / last-quitter). We treat these
// as authoritative "race is complete". This set gates the H7 anti-grief check
// only — see TERMINAL_SETTLE_STATUS for who actually settles the room.
const VERIFIED_COMPLETE_REASONS = new Set(['all_done', 'all_finished']);

// ── P2.5 Phase B0 (2026-07-29) ───────────────────────────────────────────────
// Every terminal reason a client can relay, mapped to the room status that
// client writes for it TODAY. The values mirror LiveRaceScreen/RaceResultsScreen
// exactly and must keep doing so — changing one silently rewrites race history
// and lifetime stats:
//   host_quit         -> cancelled   (LiveRaceScreen.js:2901, host quit, last racer)
//   host_cancelled    -> cancelled
//   host_ended        -> finished    (:2156 handleHostEndRace)
//   host_removed_last -> finished    (:3026 handleRemoveRacer)
//   all_done          -> finished    (:3152 runAutoEnd)
//   all_finished      -> finished    (:2812 handleFinish, :2981 handleQuit)
// The old comment here claimed host_* reasons "need no server fallback (the
// host's own RPC call succeeds)". That is true only while the client still
// writes the status. Phase B deletes those writes, so the server must own every
// one of these or the room sits 'racing' forever (BADGE2, 2026-07-13).
const TERMINAL_SETTLE_STATUS = new Map([
  ['host_quit',         'cancelled'],
  ['host_cancelled',    'cancelled'],
  ['host_ended',        'finished'],
  ['host_removed_last', 'finished'],
  ['all_done',          'finished'],
  ['all_finished',      'finished'],
]);

// Delay before the server settles a RELAYED terminal. This is a correctness
// requirement, not politeness. settle_room's R-09 marks latch dnf_final=true,
// and save_race_result refuses to write once that latch is set:
//     where race_results.finish_position is null and race_results.dnf_final is not true
// Settling the instant the terminal is relayed would therefore DROP each
// surviving racer's own result write (silently — the RPC returns success with 0
// rows) and leave them holding room_members.last_distance_m, which can lag well
// behind their real distance. Letting their write land first inverts the order:
// mark_member_lifecycle then merges via GREATEST(distance) instead of replacing
// it, so the racer keeps their true distance AND gains the DNF latch.
//
// THE VALUE IS DERIVED, NOT PICKED. The writes this grace protects are the
// client's own post-terminal save_race_result calls, and every one of them is
// wrapped in step(..., 8000): LiveRaceScreen applyRaceOver (:2133) and
// runAutoEnd (:3163). A grace shorter than that bound loses the race against a
// slow-but-successful write on exactly the connections that need it most. 9000
// clears the 8000 ceiling with a second to spare. If either step() bound moves,
// this must move with it — grep save_race_result in LiveRaceScreen.js.
const TERMINAL_SETTLE_GRACE_MS = 9000;

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
      // room, and never finalizes a race that isn't actually running. The CAS now
      // lives inside settle_room; a null verdict is the RPC-failure branch and maps
      // 1:1 onto the `error` this site checked before.
      const verdict = await settleRoom(roomId, 'finished', 'all_done');
      if (verdict === null) { room.finalizing = false; log('SERVER-FINALIZE failed', roomId); }
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
      await settleRoom(roomId, 'cancelled', 'inactivity_abandon');   // guarded: never overrides a settled room
    } catch (e) { log('ABANDON status update failed', roomId, e && e.message); }
  }
  deliverTerminal(room, roomId, { reason: 'inactivity_abandon' });
}

// ── P2 lifecycle mark (2026-07-27) ───────────────────────────────────────────
// Stamp a racer's server-owned terminal state via the service-role-only
// mark_member_lifecycle RPC (advisory-locked, stands down on a settled room or
// an existing ranked row, and writes the legacy quit/dnf race_results row in the
// same transaction).
//
// NEVER throws and never blocks a relay: every caller treats a failed mark as
// "no mark landed", which degrades to exactly the pre-P2 behaviour. That is the
// whole safety argument for calling it from the hot path.
//
// Requires 20260727_p2_member_lifecycle.sql + 20260727_p2_mark_member_lifecycle.sql
// to be APPLIED FIRST. Deployed ahead of them, every call returns a PostgREST
// error, gets logged, and changes nothing.
async function markLifecycle(roomId, userId, lifecycle, reason) {
  if (!supabase) return null;                             // TEST_MODE: fully inert
  try {
    const { data: verdict, error } = await supabase.rpc('mark_member_lifecycle', {
      p_room_id: roomId, p_user_id: userId, p_lifecycle: lifecycle, p_reason: reason,
    });
    if (error) { log('MARK rpc failed', roomId, userId, lifecycle, error.message); return null; }
    // 'no_member' is EXPECTED on a client-initiated quit: handleQuit writes its own
    // DNF row via save_race_result and then deletes the membership row, so the mark
    // can legitimately arrive after the row is gone. The result row already exists,
    // so nothing is lost — the shim matters for server-originated marks.
    log(`MARK room=${roomId} user=${userId} ${lifecycle} -> ${verdict}`);
    return verdict;
  } catch (e) { log('MARK error', roomId, userId, e && e.message); return null; }
}

// ── E-11 (2026-08-03): rehydrate a rejoining racer's terminal state ──────────
// room.racers is memory-only, so ANY loss of the room object mints a fresh `st`
// with evicted/quit/finished all false on the next JOIN. Three ways to lose it,
// all reachable mid-race: a PM2 restart (uncaughtException exits 1), a deploy,
// and the empty-room GC — which runs after EMPTY_ROOM_TTL_MS, or immediately
// once room.terminal is set.
//
// The victim is the problem. An already-DNF'd racer rejoins as a live one: the
// `evicted` send at JOIN reads the very flag that was just lost, so they are
// never told, and they resume feeding gps into the batch. Peers on the new room
// object have no record either — the E-17 client guard keys off the
// racer_quit{evicted:true} broadcast, which nothing re-sends.
//
// evictRacer already distrusts memory for exactly this reason and re-reads
// room_members.lifecycle (see its 'P2:' note). This applies that same rule at the
// one other place that decides a racer's state. The column is the durable record
// and it survives every loss listed above.
//
// DELIBERATELY NOT AWAITED by the JOIN handler: a DB round-trip must never delay
// adding the socket to the room or replaying a pinned terminal. Same idiom as the
// post-relay markLifecycle. The cost is a short window — one read — in which the
// racer is treated as active; it ends with the same two messages evictRacer sends.
//
// FAILS OPEN. A read error, a missing row, or an 'active'/absent lifecycle leaves
// today's behaviour byte-for-byte. Only a definite terminal lifecycle changes it.
async function hydrateRacerFromDb(roomId, room, userId, st) {
  if (!supabase) return;                                  // TEST_MODE: fully inert
  try {
    const { data: m, error } = await supabase.from('room_members')
      .select('lifecycle, lifecycle_reason, lifecycle_at').eq('room_id', roomId).eq('user_id', userId)
      .eq('role', 'racer').maybeSingle();
    if (error) { log('HYDRATE read failed', roomId, userId, error.message); return; }
    if (!m || !m.lifecycle || m.lifecycle === 'active') return;
    // The room may have been replaced or torn down while the read was in flight,
    // and the racer's own state may have advanced on a relay. Memory wins then.
    if (rooms.get(roomId) !== room || room.racers.get(userId) !== st) return;
    if (st.finished || st.quit || st.evicted) return;
    if (m.lifecycle === 'finished') {
      st.finished = true; room.hasFinisher = true;
      // finishedAt is deliberately left unset. It drives the rowless-finisher
      // dwell in maybeCompleteRace, and stamping it here would start that clock
      // from the rejoin rather than from the finish this server never witnessed.
      // Absent, the term simply cannot fire (fail-closed) and the racer still
      // needs a real result row to resolve — which is the correct outcome.
      log(`HYDRATE room=${roomId} user=${userId} finished`);
      return;
    }
    if (m.lifecycle === 'quit') {
      st.quit = true;
      log(`HYDRATE room=${roomId} user=${userId} quit`);
      return;
    }
    // 'dnf' — restore the eviction and re-send BOTH halves evictRacer sends, so
    // the victim learns they are out and peers render the ✕ (E-17). Idempotent
    // on the client: quitUsersRef is a set.
    st.evicted = true;
    st.evictReason = m.lifecycle_reason || 'disconnect_timeout';
    // E-13 telemetry: recover the eviction instant from the durable column, since
    // the in-memory st.evictedAt died with the room object. mark_member_lifecycle
    // stamps lifecycle_at in the same transaction as lifecycle, so it is the same
    // instant evictRacer would have recorded. Null-guarded: pre-P2 rows have none.
    const evictedAtMs = m.lifecycle_at ? new Date(m.lifecycle_at).getTime() : 0;
    if (evictedAtMs) st.evictedAt = evictedAtMs;
    let victimWs = null;
    for (const c of room.clients) if (c.userId === userId) victimWs = c;
    broadcast(room, { event: 'racer_quit', payload: { user_id: userId, evicted: true } }, victimWs);
    if (victimWs) send(victimWs, { event: 'evicted', payload: { user_id: userId, reason: st.evictReason } });
    log(`HYDRATE room=${roomId} user=${userId} evicted reason=${st.evictReason}`);
    // Reaching here at all means the racer reconnected after being evicted — the
    // same measurement the memory path emits, so one grep covers both.
    if (evictedAtMs) log(`EVICT-RECONNECT room=${roomId} user=${userId} reason=${st.evictReason} after_ms=${Date.now() - evictedAtMs} src=hydrate`);
  } catch (e) { log('HYDRATE error', roomId, userId, e && e.message); }
}

// ── P2.5 Phase A settle (2026-07-28) ─────────────────────────────────────────
// Settle a room through the service-role-only settle_room RPC instead of a bare
// guarded UPDATE. One transaction does three things the five settle sites cannot
// do for themselves:
//   1. the SAME CAS they use today (.eq('status','racing')), in-transaction;
//   2. the R-09 marks — racers this settle is about to end who never quit and
//      never finished get a 'dnf' mark + result row. These MUST happen before the
//      status write: mark_member_lifecycle returns 'room_settled' and writes
//      nothing once status <> 'racing', so marking from deliverTerminal (every
//      call site of which runs AFTER the status write) would always no-op;
//   3. the race-voice storage cleanup, which until now only ever ran on the
//      client's update_room_status path — every server-originated settle leaked
//      its recordings.
//
// Verdict: 'settled:<n>' (we won the CAS, n racers marked) | 'lost' (someone else
// settled first) | 'no_room' | 'bad_status'. Returns null on RPC failure, which
// callers treat exactly as today's `error` branch.
//
// Requires 20260728_p25a_settle_room.sql to be APPLIED FIRST. Deployed ahead of
// it, every call returns a PostgREST error and the room is NOT settled — so this
// deploy is gated on the DDL, unlike the P2 mark whose failure was inert.
async function settleRoom(roomId, status, reason) {
  if (!supabase) return null;                             // TEST_MODE: fully inert
  try {
    const { data: verdict, error } = await supabase.rpc('settle_room', {
      p_room_id: roomId, p_status: status, p_reason: reason,
    });
    if (error) { log('SETTLE rpc failed', roomId, status, error.message); return null; }
    log(`SETTLE room=${roomId} ${status} -> ${verdict}`);
    return verdict;
  } catch (e) { log('SETTLE error', roomId, e && e.message); return null; }
}
const settleWon = (v) => typeof v === 'string' && v.startsWith('settled:');

// ── P2.5 Phase B0 (2026-07-29) ───────────────────────────────────────────────
// Settle a room the server has just relayed a terminal for. Today this is pure
// redundancy: the client that sent the race_over also wrote the status, so
// settle_room's CAS returns 'lost' and this is a no-op. That is exactly what
// makes B0 safe to deploy AHEAD of the client OTA — it cannot change the
// outcome for an un-adopted client, it only removes the dependency on one.
// After Phase B deletes the client writes, this becomes the settle.
//
// Deliberately NOT cancelled by room GC. When every survivor drops their socket
// right after the terminal, teardown used to abort the pending status write and
// the room sat 'racing' forever (BADGE2 2026-07-13); settleRoom is DB-only and
// safe to run against a torn-down room, so the timer must outlive the room.
function scheduleTerminalSettle(roomId, room, reason) {
  const status = TERMINAL_SETTLE_STATUS.get(reason);
  if (!status) return;                       // unknown/absent reason → not ours to settle
  if (room.terminalSettleTimer) return;      // one per room; repeat race_over relays are no-ops
  room.terminalSettleTimer = setTimeout(async () => {
    room.terminalSettleTimer = null;
    const verdict = await settleRoom(roomId, status, reason);
    // Verdict is logged by settleRoom. 'lost' is the EXPECTED result until the
    // client OTA lands and is not an error — do not add a failure branch here.
    if (settleWon(verdict)) log(`TERMINAL-SETTLE room=${roomId} ${status} reason=${reason}`);
  }, TERMINAL_SETTLE_GRACE_MS);
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
      .select('last_distance_m, lifecycle').eq('room_id', roomId).eq('user_id', userId)
      .eq('role', 'racer').maybeSingle();
    if (mErr) { log('EVICT member read failed', roomId, userId, mErr.message); return; }
    if (!m) { st.quit = true; scheduleCompletionCheck(roomId, room); return; } // row gone → already quit/kicked
    // P2: row absence is no longer the only "already resolved" signal — P1.5 keeps
    // the row while the room is racing. The in-memory guard at the top of this
    // function (st.quit / st.finished) is lost on a PM2 restart, after which this
    // would stamp dnf_final onto an already-quit racer's row. The column survives
    // the restart; trust it. Optional-chained so a deploy that lands before the
    // migration degrades to the old behaviour instead of misreading undefined.
    if (m.lifecycle && m.lifecycle !== 'active') {
      if (m.lifecycle === 'finished') { st.finished = true; room.hasFinisher = true; }
      else st.quit = true;
      log(`EVICT stand-down (lifecycle=${m.lifecycle}) room=${roomId} user=${userId}`);
      scheduleCompletionCheck(roomId, room);
      return;
    }
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
    // E-13 telemetry: stamp when the eviction landed so a later JOIN can report
    // how long the racer was gone before coming back. In-memory only — a restart
    // loses it, and the hydrate path recovers it from room_members.lifecycle_at.
    st.evictedAt = Date.now();
    // P2: evict_racer_dnf already wrote the durable row; this stamps the matching
    // participant state so a restart-blind re-evict stands down at the check above.
    // The shim's upsert is GREATEST/OR on distance and dnf_final, so it can only
    // agree with what evict_racer_dnf just wrote, never downgrade it.
    await markLifecycle(roomId, userId, 'dnf', reason);
    // E-09: deliberately NOT room.gps.delete(userId). Dropping the entry took the
    // racer out of the next position batch, so their dot VANISHED from every map
    // instead of freezing where they stopped. A marked participant stays in the
    // batch structurally; their last position is still the truth about where they
    // are, and st.evicted is what tells peers how to render them (✕, exactly like
    // a quit). Completion maths reads the DNF row, never this map, so keeping the
    // entry cannot hold a room open.
    // The other gps.delete call sites are NOT this case and stay: a kick removes
    // someone from the race entirely, and the close/disconnect paths tear the room
    // or socket down rather than marking a participant.
    // E-13 telemetry: absent_ms is the ACTUAL continuous transport absence at the
    // moment of eviction, not the configured cutoff — the two differ because the
    // sweep runs every INACTIVITY_TICK_MS and because a fallback-progress
    // stand-down restarts st.disconnectedAt. 0 on the stationary path, where the
    // racer is connected; read it together with `reason`.
    const absentMs = st.disconnectedAt ? Date.now() - st.disconnectedAt : 0;
    log(`EVICT room=${roomId} user=${userId} reason=${reason} dist=${Math.round(dist)} time=${timeMs} absent_ms=${absentMs}`);
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

// E-16 follow-up: can the server DISPROVE a client's "everyone is done" claim?
//
// VERIFIED_COMPLETE_REASONS is named for a check that happens on the CLIENT. The
// server has only ever taken the word 'all_done' as evidence that the check was
// run and passed. On 2026-08-01 (room 2ddcbd5c) a client sent it 554ms after the
// first racer crossed, and the server dutifully ended the race for an opponent
// still at 719/805m. 31bd82f closed the client-side route to that specific false
// claim; nothing stopped the server acting on a false claim from any other route.
//
// Deliberately a DISPROOF, not an independent verdict. Requiring the server to
// conclude "complete" before honouring a terminal would mean any race the server
// merely cannot confirm never ends — the E-02 hang, reintroduced server-side, and
// the server's own completion logic is intentionally WEAKER than the client's in
// places (see the dwell term in maybeCompleteRace). So this only fires when the
// server holds positive evidence to the contrary: a racer whose socket is open
// right now and who has no terminal state at all. That is the same notion
// maybeCompleteRace already encodes — no result row and still connected means
// unresolved — so this adds no new concept, only a new place it is enforced.
//
// In-memory by design: no DB round-trip, so terminal delivery for a legitimate
// race end keeps its current latency and cannot be delayed or blocked by a slow
// or failing read. Socket state is the server's own bookkeeping and is the one
// thing it knows better than any client.
//
// Returns the offending user_id, or null when the claim cannot be disproved.
function contradictsCompletion(room) {
  for (const [uid, st] of room.racers) {
    if (!st.connected) continue;                      // gone: maybeCompleteRace's 90s window owns them
    if (st.finished || st.quit || st.evicted || st.evicting) continue;   // resolved
    return uid;                                       // connected, racing, unresolved → not "all done"
  }
  return null;
}

async function maybeCompleteRace(roomId, room) {
  if (!supabase || room.terminal || room.completing) return;
  room.completing = true;
  try {
    const [{ data: rawMembers, error: mErr }, { data: results, error: rErr }] = await Promise.all([
      supabase.from('room_members').select('user_id, lifecycle').eq('room_id', roomId).eq('role', 'racer'),
      supabase.from('race_results').select('user_id, finish_position').eq('room_id', roomId),
    ]);
    // P2: drop ONLY the terminal-with-row states. 'finished' deliberately STAYS in
    // the roster — dropping it would relax the rule that every roster racer needs a
    // race_results row, so a racer marked finished by the relay whose
    // assign_race_position later failed would count as resolved while still
    // rowless, which is precisely the hole the dwell term below exists to close.
    // Pre-migration (lifecycle undefined) every row is kept: identical to today.
    const members = (rawMembers || []).filter(mm => mm.lifecycle !== 'quit' && mm.lifecycle !== 'dnf');
    // ZERO-FINISHER GAP (fixed 2026-08-03). This guard used to read
    // `members.length === 0`, which was correct before P2 — back then `members`
    // WAS `rawMembers`, so an empty list meant "no roster, nothing to decide".
    // The P2 filter above silently gave the same expression a second meaning:
    // EVERY roster racer is terminally marked. Treating that as "nothing to
    // decide" is exactly backwards — it is the strongest possible evidence the
    // race is over, and bailing left the room 'racing' with nobody left to end it.
    //
    // Observed, not theorised: 29 prod rooms carry cancel_reason
    // 'auto_cleanup_stale', the 3-hour cleanup_stale_rooms() cron catching what
    // this function walked away from. Room f459f7a4 (2026-07-29) sat open 187 min
    // with 0 ranked rows and 2 dnf rows, and the cron then stamped it 'finished' —
    // the all-DNF 'finished' room the C5 branch below exists to make unreachable.
    //
    // Falling through is safe and needs no new logic: `unresolved` derives from
    // `members`, so an all-marked roster yields an empty list and lands on the
    // same settle branch, which already picks 'finished' vs 'cancelled' on
    // hasFinisher. An all-dnf room therefore settles 'cancelled', per C5.
    //
    // The genuinely-empty roster KEEPS the old bail. A successful read returning
    // zero rows is not evidence of completion, and settling on it would end a live
    // race off one bad read. That case stays with the cron backstop.
    if (mErr || rErr || !rawMembers || rawMembers.length === 0) return;
    // room.meta is otherwise populated lazily on first eviction, so targetM would be
    // undefined here in most rooms and the rowless-finisher term below could never
    // fire. Hydrate on demand; a failed read leaves targetM 0, which disables that
    // term only (fail-closed — it can never resolve a racer without a real target).
    if (!room.meta) {
      const { data: r } = await supabase
        .from('race_rooms').select('started_at, target_distance_m').eq('id', roomId).single();
      if (r) room.meta = { startedAt: r.started_at, targetM: r.target_distance_m };
    }
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
      // Rowless-finisher term (client parity, strictly weaker). Resolve a racer whose
      // 'finished' relay THIS server witnessed >= FINISH_NOROW_DWELL_MS ago and whose
      // server-tracked distance reached the target, even though no row landed. Without
      // it the room hangs on the missing row until the stationary sweep writes
      // dnf_final on a real finisher. Dwell + our own witnessed relay + the existing
      // hasFinisher gate keep this strictly more conservative than the client term.
      if (st && st.finished && st.finishedAt && now - st.finishedAt >= FINISH_NOROW_DWELL_MS) {
        const targetM = room.meta?.targetM || 0;
        const dist = Math.max(room.gps.get(user_id)?.distance_m || 0, st.lastDist || 0);
        if (targetM > 0 && dist >= targetM) return false;
      }
      return !(st && !st.connected && st.disconnectedAt && now - st.disconnectedAt >= SERVER_GONE_GRACE_MS);
    });
    if (unresolved.length) return;
    // Distinguish the zero-finisher-gap path in the log: an EMPTY `members` means
    // every roster racer was terminally marked, which is the case that used to
    // bail out above and reach the 3-hour cron instead. `all_marked` in a
    // SERVER-COMPLETE line is this fix firing; its absence is the ordinary path.
    const allMarked = members.length === 0;
    if (hasFinisher) {
      // Status CAS (same guard as maybeServerFinalize): exactly one settler wins;
      // deliver the terminal only if WE won, so reasons never conflict.
      const won = await settleRoom(roomId, 'finished', 'all_done');
      if (won === null) { log('SERVER-COMPLETE failed', roomId); return; }
      if (settleWon(won)) {
        log(`SERVER-COMPLETE room=${roomId}${allMarked ? ' all_marked' : ''}`);
        deliverTerminal(room, roomId, { reason: 'all_done', server_finalized: true });
      }
    } else {
      // C5: zero finishers → a race with no outcome is 'cancelled', never an
      // all-DNF 'finished'. Reason stays 'inactivity_abandon': it is the
      // established zero-finisher terminal and clients short-circuit their stats
      // path on it, which is the correct behaviour for an all-DNF room too. A
      // truer reason would be a new string every client would have to learn.
      const won = await settleRoom(roomId, 'cancelled', 'inactivity_abandon');
      if (won === null) { log('SERVER-COMPLETE cancel failed', roomId); return; }
      if (settleWon(won)) {
        log(`SERVER-COMPLETE (no finishers → cancelled) room=${roomId}${allMarked ? ' all_marked' : ''}`);
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
      // P2: the comment above ("quit racers already deleted their row") no longer
      // holds for a server-marked quit/dnf, so exclude those explicitly or a room
      // whose racers have all left stays 'racing' forever. Only quit/dnf are
      // excluded: a 'finished' racer still on the HUD keeps their row and keeps
      // being counted, exactly as today.
      const { count, error: cErr } = await supabase.from('room_members')
        .select('user_id', { count: 'exact', head: true })
        .eq('room_id', roomId).eq('role', 'racer').neq('user_id', room.hostId)
        .not('lifecycle', 'in', '("quit","dnf")');
      if (cErr) { room.hostClosing = false; log('HOST-ABANDON count failed', roomId, cErr.message); return; }
      if ((count || 0) > 1) { room.hostClosing = false; log(`HOST-ABANDON stand-down room=${roomId} remaining=${count}`); return; }
      const verdict = await settleRoom(roomId, 'cancelled', 'host_quit');
      if (verdict === null) { room.hostClosing = false; log('HOST-ABANDON cancel failed', roomId); return; }
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
    const fresh = !st;
    if (!st) {
      st = { connected: true, disconnectedAt: 0, lastMoveTs: Date.now(), lastDist: 0,
             baselineDist: 0, warnDeadline: 0, finished: false, quit: false,
             evicted: false, goneNotified: false, evicting: false, warnChecking: false };
      room.racers.set(userId, st);
    }
    st.connected = true; st.disconnectedAt = 0; st.goneNotified = false;
    st.lastMoveTs = Date.now();                 // reconnect re-seeds the stationary clock
    // E-11: an ABSENT entry means either a genuine first join or a room object
    // this server lost (restart / GC) — only the DB can tell those apart, so ask
    // it. When `st` already existed, memory is the NEWER record (the finished /
    // quit relay stamps it before mark_member_lifecycle lands), so this must
    // never run for a live entry. A fresh entry can never be evicted yet, which
    // is why the pinned-terminal send below is the other branch, not both.
    if (fresh) void hydrateRacerFromDb(roomId, room, userId, st);
    else if (st.evicted) {
      // E-13 telemetry: the racer came back AFTER being evicted — the measurement
      // that says whether the cutoff is DNF'ing people who were still racing.
      // Emitted here rather than in applyEvicted because only the server knows
      // both timestamps. Absent evictedAt (pre-upgrade state) logs nothing.
      if (st.evictedAt) log(`EVICT-RECONNECT room=${roomId} user=${userId} reason=${st.evictReason || '?'} after_ms=${Date.now() - st.evictedAt} src=memory`);
      send(ws, { event: 'evicted', payload: { user_id: userId, reason: st.evictReason || 'disconnect_timeout' } });
    }
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
    // M-3: everything past JSON.parse runs inside this guard so a malformed or
    // unexpected frame kills only THIS message, not the process. An uncaught
    // throw here would drop every room on the server. Never swallowed — the
    // catch logs the offending event type + error.
    try {
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
            // E-16: a 'verified' reason is only the client's word for it. If the
            // server can positively disprove the claim, drop it — but still
            // re-arm its own completion check, so a room that genuinely IS
            // complete settles through maybeCompleteRace (which delivers its own
            // terminal) instead of stalling on the rejection.
            if (verified) {
              const stillRacing = contradictsCompletion(room);
              if (stillRacing) {
                log(`RACE_OVER rejected (contradicted) room=${roomId} from=${ws.userId} reason=${payload.reason} still_racing=${stillRacing}`);
                scheduleCompletionCheck(roomId, room);
                return;
              }
            }
            deliverTerminal(room, roomId, payload, ws);
            if (verified) maybeServerFinalize(roomId, room);
            // P2.5 Phase B0: the server now settles every relayed terminal,
            // instead of trusting the sender's client to have done it. Runs
            // alongside maybeServerFinalize on purpose — both are CAS-guarded by
            // settle_room, so at most one wins and terminal delivery above is
            // untouched either way.
            scheduleTerminalSettle(roomId, room, payload.reason);
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
          if (st) {
            if (event === 'finished') {
              // finishedAt stamps the FIRST witnessed finish only, so the dwell window
              // cannot be reset by a repeat relay (handleFinish resends on a landed
              // retry, and raceSendReliable itself retries).
              if (!st.finished) st.finishedAt = Date.now();
              st.finished = true;
            } else st.quit = true;
          }
          if (event === 'finished') room.hasFinisher = true;
          scheduleCompletionCheck(roomId, room);
          // P2: stamp the participant state, then RE-ARM the completion check.
          // The unconditional schedule above is kept exactly as it was, so a slow
          // or failing RPC can never cost us a check; this second, awaited one
          // exists because the debounced check can otherwise fire BEFORE the mark
          // (and its shim row) lands, find the racer unresolved, and not re-run
          // until some later event happens to schedule it again.
          // Deliberately not awaited by the handler: the broadcast below must not
          // wait on a DB round-trip.
          void (async () => {
            await markLifecycle(roomId, ws.userId,
              event === 'finished' ? 'finished' : 'quit',
              event === 'finished' ? 'finish_relay' : 'client_quit');
            scheduleCompletionCheck(roomId, room);
          })();
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
    } catch (e) {
      // M-3: one malformed/unexpected frame must kill only THIS message, never
      // the process (which would drop every room). Log type + error; never silent.
      log(`MESSAGE-DISPATCH error user=${ws.userId} room=${ws.roomId} event=${(msg && msg.event) || '?'} err=${e && e.message}`);
      if (e && e.stack) console.error(e.stack);
      captureServer(e, { guard: 'dispatch', event: (msg && msg.event) || '?', room_id: ws.roomId });  // D-08
    }
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
  // M-3: a throw in this sweep must not take the process down (it runs every
  // room). Contain + log which sweep threw; the next tick recovers.
  try {
    for (const room of rooms.values()) {
      if (!room.dirty || room.gps.size === 0) continue;
      broadcast(room, {
        event: 'gps_batch',
        payload: { racers: Array.from(room.gps.values()), ts: Date.now() },
      });
      room.dirty = false;
    }
  } catch (e) {
    log(`SWEEP error timer=batch err=${e && e.message}`);
    if (e && e.stack) console.error(e.stack);
    captureServer(e, { guard: 'sweep', timer: 'batch' });     // D-08
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
  // M-3: contain a throw so one bad room can't kill the whole lifecycle sweep
  // (and with it every room's eviction/completion timing).
  try {
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
  } catch (e) {
    log(`SWEEP error timer=inactivity err=${e && e.message}`);
    if (e && e.stack) console.error(e.stack);
    captureServer(e, { guard: 'sweep', timer: 'inactivity' }); // D-08
  }
}, INACTIVITY_TICK_MS);

// ── Ping/pong: terminate connections that miss a heartbeat ───────────────────
const pingTimer = setInterval(() => {
  // M-3: contain a throw so the heartbeat sweep can't take the process down.
  try {
    for (const ws of allClients) {
      if (ws.isAlive === false) {
        log('TERMINATE dead connection', ws.userId);
        ws.terminate(); // fires 'close' -> handleClose cleans up
        continue;
      }
      ws.isAlive = false;
      try { ws.ping(); } catch (e) {}
    }
  } catch (e) {
    log(`SWEEP error timer=ping err=${e && e.message}`);
    if (e && e.stack) console.error(e.stack);
    captureServer(e, { guard: 'sweep', timer: 'ping' });      // D-08
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

// ── Last-resort crash guards (M-3) ──────────────────────────────────────────
// The per-message dispatch and the three sweep timers are now individually
// guarded, so these catch the residual: any other unexpected throw, and the
// fire-and-forget async IIFEs (e.g. the race_over finalize). Without them,
// Node 20 terminates the whole process on either event, dropping every
// in-flight room at once. The two events are handled asymmetrically on purpose.
process.on('uncaughtException', (err, origin) => {
  // State is now undefined — the safe move is a clean exit + PM2 restart (~1-2s),
  // NOT limping on in an unknown state. This is already Node's default; we add
  // the structured context the raw crash never logged.
  try { log(`FATAL uncaughtException origin=${origin} name=${err && err.name} msg=${err && err.message}`); } catch (_) {}
  if (err && err.stack) { try { console.error(err.stack); } catch (_) {} }
  // D-08: this is the ONE capture that cannot use the fire-and-forget helper —
  // the process is about to exit, and an unflushed event is simply lost. Sentry
  // is given a bounded 1500 ms to ship it, then we exit regardless.
  //
  // ⚠️ THIS DELAYS THE EXIT, which the comment above deliberately wants prompt
  // ("the safe move is a clean exit + PM2 restart, NOT limping on in an unknown
  // state"). 1500 ms is the compromise: long enough for one HTTPS round trip,
  // short against PM2's own restart time, and it only ever applies on a crash
  // that is already fatal. Both settle paths exit 1 — a flush that fails must
  // never turn a crash into a hang.
  if (Sentry) {
    try {
      Sentry.captureException(err, { tags: { origin }, level: 'fatal' });
      Sentry.close(1500).then(() => process.exit(1), () => process.exit(1));
      return;
    } catch (_) {}
  }
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  // A stray rejected promise rarely corrupts global state; killing every live
  // race for it is disproportionate. Log with context and KEEP RUNNING.
  try {
    const r = reason instanceof Error ? `${reason.name}: ${reason.message}` : String(reason);
    log(`unhandledRejection (non-fatal, kept running) reason=${r}`);
    if (reason instanceof Error && reason.stack) console.error(reason.stack);
    captureServer(reason, { guard: 'unhandledRejection' });   // D-08
  } catch (_) {}
});
