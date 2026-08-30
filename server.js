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

// ── R-61: the server owns the distance ───────────────────────────────────────
//
// 🔑 THE DEFECT THIS CLOSES. `gps` used to buffer `payload.distance_m` verbatim —
// no lower bound, no displacement cap, no comparison against elapsed time. Every
// other anti-cheat layer in this product inspects SENSOR HONESTY on the device
// (T8 steps, R-11's mock-provider verdict, the whole T7 filter chain), so all of
// them are bypassed completely by a client that never uses its sensors and simply
// asserts a distance. The finish-time guards do not catch it either:
// assign_race_position asks for >= 90% of target (a forged full distance satisfies
// it) and an average speed within cap x 1.5 (a forged distance over the real
// elapsed time is entirely plausible). One forged frame won the race.
//
// The fix is the model Strava uses and the one this server should always have had:
// the client reports, the SERVER decides. Each frame may advance a racer by at most
// what the activity's speed cap allows for the time that has actually passed.
//
// 🔑 THE BUDGET IS MEASURED IN ELAPSED WALL TIME, NOT PER MESSAGE. This is the
// single most important property here and it is the same lesson as the §4e delivery
// clock on the client. A per-message cap would trim the honest catch-up of a racer
// returning from a signal drop or an Android delivery batch — punishing the WORST
// connection hardest, and (because a stalled distance is what the stationary
// eviction watches) eventually DNF-ing them. Budgeting on elapsed time means a
// 3-minute tunnel legitimately buys 3 minutes of catch-up, while an instant jump
// 5 seconds into the race buys 5 seconds' worth.
//
// ⚠️ THREE SAFETY PROPERTIES, all of them about not harming honest racers:
//   1. TRIM, NEVER REJECT. A frame over budget is admitted at the ceiling, not
//      dropped. Dropping would freeze the racer's progress entirely.
//   2. NEVER GO BACKWARDS. The accepted distance is monotone, so a trim can never
//      reduce a racer below ground already granted (which would read as "no
//      progress" and route them to eviction — the exact cost the cheat has).
//   3. ABSTAIN WHILE UNINFORMED. Until the room's activity type and start time are
//      known, nothing is trimmed. An unknown room is treated as honest.
//
// Caps mirror the client's SPORT_SPEED_CAPS with the same x1.5 margin
// assign_race_position uses, so nothing here is stricter than a check that has
// already been live for a year.
const SERVER_DISTANCE_BUDGET = true;             // kill switch — restart to revert
const BUDGET_SPEED_CAPS_KMH  = { running: 50, cycling: 100, walking: 20, swimming: 10 };
const BUDGET_MARGIN          = 1.5;              // matches assign_race_position
const BUDGET_UNKNOWN_CAP_KMH = 100;              // most permissive, used pre-hydration
// A frame may always advance this far regardless of dt, so that clock skew or a
// sub-second tick can never trim a legitimate GPS step to nothing.
const BUDGET_FLOOR_M         = 25;

// ── R-64 Part 3, PHASE 1: stamp the crossing. MEASUREMENT ONLY ───────────────
// Writes race_crossings when a racer's ACCEPTED distance first reaches the target.
// Nothing reads it; no client behaviour, no ranking, no eviction depends on it.
// See supabase/sql/20260814_r64p3_crossing_measure.sql for why it is a phase.
const SERVER_CROSSING_STAMP = true;              // kill switch — restart to revert

// ── Crossing-resolve: the crossing IS the finish (2026-08-18) ────────────────
// resolve_crossing_finish (service-role-only RPC) writes the RANKED race_results
// row the instant this relay witnesses the accepted distance reach the target.
// A locked/JS-suspended racer's claim then becomes a CONFIRMATION at unlock
// instead of the thing the whole room waits on (rooms f803900c/b50bebc5,
// 2026-08-18: "finishing…" held ~4.7 min until unlock, then the honest claims
// were flagged stale_time_claim). source='baseline' crossings are excluded —
// the real crossing happened where this relay could not see it.
// Requires supabase/sql/20260818_crossing_resolve_finish.sql APPLIED FIRST;
// deployed against the old DB every call fails closed to a logged RPC error.
const SERVER_CROSSING_RESOLVE = process.env.SERVER_CROSSING_RESOLVE !== '0';  // kill switch

// ── Canonical replay curve (2026-08-18) ──────────────────────────────────────
// The race preview replayed each device's LOCAL frame buffer, so every racer
// saw a different movie (per-device sampling, Doze holes, own-credited vs
// server-budgeted distance). This relay is the only observer identical for all
// racers: sample each racer's BUDGETED distance every REPLAY_STEP_MS and flush
// one race_replay_curves row per racer at room terminal — the flushShadow
// pattern exactly. Requires supabase/sql/20260818_replay_curves.sql APPLIED
// FIRST; against the old DB every flush fails closed to a logged upsert error.
const SERVER_REPLAY_CURVE  = process.env.SERVER_REPLAY_CURVE !== '0';  // kill switch
const REPLAY_STEP_MS       = 5000;
const REPLAY_MAX_SAMPLES   = 2880;   // 4h at 5s — per-racer memory bound

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
// ── R-49: server-authoritative presence ──────────────────────────────────────
// The client's red 'waiting…' verdict is derived purely from broadcast age, and
// Android's screen-off freeze stops both location delivery AND every JS timer —
// so no client-side mechanism (R-42's heartbeat included) can emit during the
// hole it is supposed to report on. THE SERVER ALREADY HOLDS THE FACT: measured
// room 75df90af, the socket stayed open through 2-10 min stall episodes (one
// JOIN, no 1006, LEAVE 1000 at race end). This publishes that fact so clients
// can distinguish 'socket open, no data' (frozen/batching: neutral no-signal
// state) from 'socket closed' (the existing REQ1 eviction clock).
//
// ⚠️ 'conn: true' IS NOT 'alive'. A connected-but-silent racer is still governed
// by the REQ2 stationary eviction (10 min + 60 s warn) — presence changes how
// peers RENDER the gap, never whether the server acts on it. Purely additive:
// old clients drop unknown events on their default case, so this deploys ahead
// of any OTA and is inert until a client opts in.
//
// PRESENCE-GAP / PRESENCE-RESUME log lines are the device-test readout for the
// R-49 precondition (does a freeze longer than 10 min keep the socket open?):
// one line when a connected racer's data gap first exceeds the threshold, one on
// resume with the episode length. Bounded to one pair per episode.
const SERVER_PRESENCE       = true;              // kill switch — restart to revert
const PRESENCE_INTERVAL_MS  = 5000;              // fan-out cadence (client frozen gate is 30s)
const PRESENCE_GAP_LOG_MS   = 30000;             // episode logging threshold = client's GPS_FROZEN_MS

// ── Phase 3 SHADOW MODE (2026-08-18) — server-side distance scoring ─────────
// Clients piggyback raw GPS fixes (payload.fx = [[ts, lat, lng, acc, spd], …],
// replayed background fixes included) on the existing 'gps' message. The server
// scores them through ONE deterministic pipeline per racer and, at room
// terminal, records {raw_m, cred_m, client_m, …} in race_shadow_distance plus a
// SHADOW log line. Nothing downstream reads any of it — the room still runs
// entirely on the client-credited (R-61-budgeted) distance. The point is a
// week of real races measuring client-vs-server deltas BEFORE the server
// number ever becomes authoritative. v1 gates are deliberately minimal
// (accuracy / ordering / teleport): the client's stationary/deadband gates are
// the ones under investigation, so the shadow must NOT replicate them.
const SERVER_SHADOW_DISTANCE   = true;   // kill switch — restart to revert

// ── LiveKit voice sweep (loitering audit 2026-08-17, fixed 2026-08-18) ──────
// Every 5 min the relay invokes the livekit-sweep Edge Function with its own
// service-role key; the function deletes LiveKit rooms whose race is cancelled
// / finished >10 min / row-less, force-disconnecting loiterers. Lives here
// (not in pg_cron) because a cron command would have to embed the service key
// in the cron table — the exact exposure D-04 removed. Relay restarts cost at
// most one 5-min cycle.
const LIVEKIT_SWEEP_ENABLED     = true;  // kill switch — restart to revert
const LIVEKIT_SWEEP_INTERVAL_MS = 5 * 60 * 1000;
// Spectator sockets must not hold a terminal room open (they count toward
// clients.size, so one backgrounded viewer kept full room state — racer maps,
// shadow + replay buffers — in memory indefinitely and delayed the room_closed
// flushes). After the race ends: a spectator that ACKED the terminal is closed
// once the finish pills have had their moment; one that never acks (dead or
// wedged socket) is closed on the hard deadline regardless. Racer sockets are
// never touched here — results/terminal redelivery still needs them.
const SPECTATOR_TERMINAL_LINGER_MS      = 60 * 1000;       // acked spectators
const SPECTATOR_TERMINAL_HARD_CLOSE_MS  = 5 * 60 * 1000;   // unacked fallback
const SHADOW_MAX_FX_PER_MSG    = 120;    // per-message cap (client sends ≤120)
const SHADOW_MAX_FIXES         = 14400;  // per racer (~4h at 1/s) — memory bound
const SHADOW_MAX_ACC_M         = 40;     // worse accuracy contributes nothing
const SHADOW_TELEPORT_MPS      = 15;     // >54 km/h chord speed = re-anchor, no credit

// ── Phase B (v2) — cred_m becomes GATED (2026-08-20). STILL SHADOW-ONLY. ────
// v1 measured cred==raw and proved raw path length is not rankable (LG +185%,
// racer2 +13..30% over client on 18–20 Aug races). v2 makes cred_m the number
// Phase C would flip to: smoothed-track crediting instead of raw chords.
// Full rationale + exit criteria: SHADOW_PHASE_B_DESIGN.md. Pipeline:
//   v1 sanity gates → CV Kalman (port of src/lib/gpsKalman.js, Phase-1 params,
//   adaptation OFF — it measured worse on-device) → positional stationarity
//   (anchor-ball dwell, NEVER reported Doppler speed — R-69's slow-walker
//   lesson) → anchor-quantum crediting with a noise floor → 12 m/s cap.
// raw_m keeps its v1 meaning so the delta series continues uninterrupted.
const SERVER_SHADOW_V2       = true;   // kill switch — false reverts cred_m to the v1 raw mirror
const SHADOW_K_PROCESS_ACC   = 0.1;    // = client GPS_KALMAN_PROCESS_ACC
const SHADOW_K_ACC_SCALE     = 1.0;    // = client GPS_KALMAN_ACC_SCALE
const SHADOW_K_MAX_GAP_MS    = 30000;  // dt clamp for the predict step (numerical safety)
const SHADOW_K_INIT_VEL_VAR  = 4.0;    // (2 m/s)^2
const SHADOW_K_ZUPT_VEL_VAR  = 0.09;   // (0.3 m/s)^2 — "stopped, fairly sure", see client note
const SHADOW_FLOOR_M         = 3.5;    // = client GPS_NOISE_FLOOR_M (credit quantum floor)
const SHADOW_FLOOR_ACC_K     = 0.5;    // = client GPS_NOISE_ACCURACY_K
// Stationary = the smoothed track's NET displacement over a SLIDING
// SHADOW_STILL_WINDOW_MS window is under SHADOW_STILL_NET_M. Sliding, not
// anchor-dwell: a parked phone's error drifts slowly (~2–3 m / 15 s), which
// keeps escaping a fixed anchor ball and never accrues the dwell, while the
// sliding question "how far in the last 15 s" answers still immediately.
// 5 m: a 2 km/h walker covers 8.3 m / 15 s — 1.7x margin even with filter
// lag (the R-69 protected case). Positional by design: Doppler is telemetry
// only, never a gate.
const SHADOW_STILL_WINDOW_MS = 15000;
const SHADOW_STILL_NET_M     = 5;
const SHADOW_MAX_CREDIT_MPS  = 12;     // = client GPS_MAX_CREDIT_MS; cycling out of scope
// A delivery gap longer than this re-seeds the filter and anchors: v2 never
// credits across a suspension gap (raw_m still measures the gap chord, as v1 did).
const SHADOW_RESET_GAP_MS    = 30000;
// ── Starvation credit rung (the R-118 residual; DRY-RUN by default) ──────────
// A battery-managed phone delivers fixes in sparse bursts — sometimes 5-14 for
// a whole race — and the reset above forfeits every inter-burst chord, so a
// starved-but-moving racer credits ~0 (locked iPhone, room "Testing 9": 33 m
// credited over 13.5 min while the carrier walked ~600). This rung recovers the
// FORFEITED GAP CHORD only: the NET displacement between a >RESET_GAP gap's
// endpoint fixes, capped at gap_time x SHADOW_MAX_CREDIT_MPS and credited only
// when it clears both endpoints' noise floors. A stationary starved phone nets
// ~0 by construction (its sparse fixes share one spot); net displacement can
// never exceed what a live stream would have credited; the rawM clamp in the
// ladder still binds on top. Teleport resets zero v2LastTs, so a glitch fix
// can never be a gap endpoint. starvM is ALWAYS accumulated + logged + flushed
// (meta.starv) — SERVER_STARVATION_LADDER=1 is the separate flip that lets it
// SCORE (rung 1 becomes seedM + min(credM + starvM, rawM)). Default 0 = dry
// run: read the would-be credit off telemetry before any result depends on it.
const SERVER_STARVATION_LADDER = process.env.SERVER_STARVATION_LADDER === '1';
// ── Phase C (SHADOW_PHASE_C_DESIGN.md) — authoritative-distance ladder ───────
// C0 = dry-run: the ladder is computed and logged on every frame but the room
// still runs on the budgeted client claim. C1 = flip SERVER_AUTHORITATIVE_DISTANCE
// to true (one constant, one reload to revert). The budget path keeps running
// underneath in both modes — it is rung 2 of the ladder and must stay warm.
const SERVER_AUTH_DRYRUN          = true;   // C0: compute + log AUTH-DELTA
const SERVER_AUTHORITATIVE_DISTANCE = true;  // C1 LIVE 2026-08-22 — C0 exit criteria passed (6 organic races, rung-1 100%, drift in band); revert = false + reload
const AUTH_FX_STALE_MS            = 20000;  // no fx for this long → fall to rung 2
// ── R-107 finish-claim guard ─────────────────────────────────────────────────
// 2026-08-22 race ae772fe4: a racer whose fix stream was degraded all race
// (rung 1 fresh, but every step below the stillness clamp) held auth=0 while
// the client's own accumulator reached target — the client then self-finished
// and its direct race_results write took pos=1 with ZERO server credit and no
// CROSSING. Until Phase B deletes the client ranked write, the relay cannot
// block that row; it CAN see the divergence at claim time and stamp the row
// flagged. Fires only on POSITIVE evidence: authoritative mode on, target
// known, the ladder observed for >= MIN_FRAMES frames, and auth credit under
// MIN_FRAC of target. A real server crossing sets authDist = target before
// any honest claim, and rung 2 tracks the budgeted claim itself, so neither
// can trip this — only a starved-but-fresh rung-1 racer can.
const FINISH_CLAIM_GUARD      = process.env.FINISH_CLAIM_GUARD !== '0';  // kill switch
const FINISH_CLAIM_MIN_FRAC   = 0.5;   // auth credit below this share of target = divergent
const FINISH_CLAIM_MIN_FRAMES = 30;    // ladder frames seen before the guard may speak
// R-107 hardening (2026-08-22): a divergent finish is DEMOTED (unranked +
// flagged, podium surrendered via demote_unwitnessed_finish) instead of only
// flagged. The RPC refuses when a witnessed live crossing exists for the racer
// — 22-Aug backtest: every honest crossing-less ranked finish had auth >= 95%
// of target, far above the 50% trip wire, so no historical honest finish would
// have demoted. Second trip wire: >= MIN_FRAMES gps frames observed but ZERO
// embedded fixes ever ingested ('no_server_fixes') — an unmodified client
// always piggybacks fx on the gps message, so frames-without-fixes is positive
// forgery evidence rung 2 otherwise waves through. Kill switch reverts to the
// flag-only behavior; the DB function stays inert then.
const FINISH_CLAIM_DEMOTE     = process.env.FINISH_CLAIM_DEMOTE !== '0';
// ── Display blend (client-side-prediction pattern, server-side placement) ────
// 2026-08-22 race c200d981: the pocketed realme's auth credit lagged its own
// honest budget by up to ~85 m while the stillness clamp flapped (stillSec=195),
// so every HUD showed it trailing and catching up in bursts. The still-gate
// sweep (scripts/replay-still-tune.mjs) proved the gate itself cannot be
// loosened without breaking the parked / 2 km/h guards — so the fix is the
// standard prediction+reconciliation split: the number the room DISPLAYS may
// run a bounded distance ahead of the number the room SCORES. displayM =
// max(auth, min(budget, auth + LEAD)) — budget is the racer's own budgeted
// claim (rung 2, speed-capped), so an honest racer displays their true
// distance while a forged claim shows at most LEAD m of phantom, with zero
// scoring effect. Below target the display is additionally held FINISH_HOLD m
// short so the C2 finish gate still lands exactly with the server CROSSING.
// Blend applies ONLY at the gps fan-out; crossings, replay curves, eviction
// progress, R-107 and results all read pure authM. Restart to revert.
const SERVER_DISPLAY_BLEND = process.env.SERVER_DISPLAY_BLEND !== '0';   // kill switch
const DISPLAY_LEAD_M       = 50;   // max metres display may lead verified credit
const DISPLAY_FINISH_HOLD_M = 3;   // held short of target until the real crossing

// Persist the raw fix stream (race_shadow_fixes, service-role only) so v2 gate
// constants can be tuned offline against REAL device traces — the synthetic
// harness false-greened the LG K61 (R-101). Capture is pre-drop: teleports and
// acc-rejects are exactly what tuning needs to see. Kill switch, no client path.
const SHADOW_PERSIST_FIXES   = true;
const SHADOW_NIS_SAMPLE_MAX  = 3000;   // bounded like the client's census sampling

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
             // R-61: carried so the budget's trim log can name the room. Every other
             // consumer already had roomId in scope; budgetedDistance is pure and
             // does not.
             id: roomId,
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
  room.terminal = { payload, mid: `${roomId}:${Date.now()}`, at: Date.now() };
  log(`TERMINAL room=${roomId} reason=${payload && payload.reason}`);
  flushShadow(roomId, room, 'terminal');
  flushReplayCurves(roomId, room, 'terminal');
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
// ── Phase 3 shadow scoring ───────────────────────────────────────────────────
function haversineM(lat1, lng1, lat2, lng2) {
  const R = 6371000, toRad = Math.PI / 180;
  const dLat = (lat2 - lat1) * toRad, dLng = (lng2 - lng1) * toRad;
  const a = Math.sin(dLat / 2) ** 2 +
            Math.cos(lat1 * toRad) * Math.cos(lat2 * toRad) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(Math.min(1, a)));
}

// ── v2: constant-velocity Kalman, per racer, INLINE because deploy.sh ships
// exactly one file. Faithful port of src/lib/gpsKalman.js (two decoupled 1-D
// filters in a local metre frame; the client header carries the full math
// rationale) minus the pieces v2 does not use: the adaptive process noise
// (shipped OFF on the client — it detects motion, not turns) and the
// moving/still path split (v2 does its own split downstream).
function createShadowKalman() {
  let lat0 = null, lng0 = null, mPerDegLng = 0, lastTs = 0, started = false;
  const ax = { p: 0, v: 0, P00: 0, P01: 0, P11: 0 };
  const ay = { p: 0, v: 0, P00: 0, P01: 0, P11: 0 };
  const predict = (a, dt) => {
    const s2 = SHADOW_K_PROCESS_ACC * SHADOW_K_PROCESS_ACC;
    const dt2 = dt * dt, dt3 = dt2 * dt, dt4 = dt2 * dt2;
    a.p += a.v * dt;
    const P00 = a.P00 + 2 * dt * a.P01 + dt2 * a.P11 + s2 * dt4 / 4;
    const P01 = a.P01 + dt * a.P11 + s2 * dt3 / 2;
    const P11 = a.P11 + s2 * dt2;
    a.P00 = P00; a.P01 = P01; a.P11 = P11;
  };
  const update = (a, z, R) => {
    const S = a.P00 + R;
    const innov = z - a.p;
    const k0 = a.P00 / S, k1 = a.P01 / S;
    const P00 = a.P00, P01 = a.P01;
    a.p += k0 * innov;
    a.v += k1 * innov;
    a.P00 = P00 - k0 * P00;
    a.P01 = P01 - k0 * P01;
    a.P11 = a.P11 - k1 * P01;
    return (innov * innov) / S;
  };
  return {
    // Fix ts, never wall clock: a replayed batch shares one arrival instant
    // while spanning minutes (the client's §2c trap).
    step(la, ln, ac, t) {
      const R0 = Math.max(1, (Number.isFinite(ac) ? ac : 10) * SHADOW_K_ACC_SCALE);
      if (!started) {
        lat0 = la; lng0 = ln;
        mPerDegLng = 111320 * Math.cos(la * Math.PI / 180);
        ax.p = 0; ax.v = 0; ax.P00 = R0 * R0; ax.P01 = 0; ax.P11 = SHADOW_K_INIT_VEL_VAR;
        ay.p = 0; ay.v = 0; ay.P00 = R0 * R0; ay.P01 = 0; ay.P11 = SHADOW_K_INIT_VEL_VAR;
        lastTs = t; started = true;
        return { x: 0, y: 0, nis: null };
      }
      const zx = (ln - lng0) * mPerDegLng;
      const zy = (la - lat0) * 110540;
      const dt = Math.min(Math.max(0, t - lastTs), SHADOW_K_MAX_GAP_MS) / 1000;
      lastTs = t;
      predict(ax, dt); predict(ay, dt);
      const R = R0 * R0;
      const nis = update(ax, zx, R) + update(ay, zy, R);
      return { x: ax.p, y: ay.p, nis };
    },
    // Zero-velocity update: applied AFTER step so the position correction
    // lands — asserting "not moving", not "not here".
    zupt() {
      ax.v = 0; ay.v = 0;
      ax.P11 = SHADOW_K_ZUPT_VEL_VAR; ax.P01 = 0;
      ay.P11 = SHADOW_K_ZUPT_VEL_VAR; ay.P01 = 0;
    },
  };
}

// Re-seed the v2 pipeline (teleport or delivery gap). The old filter state
// describes a track that is glitched or minutes stale; carrying it forward
// would mint smoothed length no real movement produced.
function shadowV2Reset(sh) {
  sh.k = null; sh.sx = null; sh.sy = null;
  sh.win = []; sh.credAnchor = null;
  sh.pendM = 0; sh.still = false; sh.v2LastTs = 0;
}

// One accepted fix through the v2 pipeline: smooth → stationarity → credit.
function shadowV2Step(sh, t, la, ln, ac, spd) {
  // Doppler is TELEMETRY ONLY, never a gate — R-69: the LG's Doppler reads ~0
  // at slow walk, and speed-gating it starved 2 km/h walkers on the client.
  if (Number.isFinite(spd)) { if (spd < 0.8) sh.spd.lo++; else sh.spd.hi++; }
  else sh.spd.na++;
  if (sh.v2LastTs && t - sh.v2LastTs > SHADOW_RESET_GAP_MS) {
    // Starvation gap-chord (see SERVER_STARVATION_LADDER): before the reset
    // forfeits this gap, bank its bounded net chord. sh.last is still the
    // PREVIOUS accepted fix here (the caller assigns it after this returns),
    // and it is the same fix v2LastTs describes: acc-drops advance neither,
    // and a teleport zeroes v2LastTs so this branch cannot run off one.
    if (sh.last && Number.isFinite(sh.last.la)) {
      const gapS = (t - sh.v2LastTs) / 1000;
      const chord = haversineM(sh.last.la, sh.last.ln, la, ln);
      const accPrev = Number.isFinite(sh.last.ac) ? sh.last.ac : 10;
      const accCur  = Number.isFinite(ac) ? ac : 10;
      // Both endpoints' noise floors: a parked starved phone whose two burst
      // positions wander a few metres apart must net to zero credit.
      const noise = Math.max(SHADOW_FLOOR_M, SHADOW_FLOOR_ACC_K * accPrev)
                  + Math.max(SHADOW_FLOOR_M, SHADOW_FLOOR_ACC_K * accCur);
      const credit = Math.min(chord, gapS * SHADOW_MAX_CREDIT_MPS);
      if (credit > noise) {
        sh.starvM += credit;
        sh.starvN++;
        sh.starvSec += Math.round(gapS);
      }
    }
    shadowV2Reset(sh);
  }
  if (!sh.k) sh.k = createShadowKalman();
  const r = sh.k.step(la, ln, ac, t);
  const prevTs = sh.v2LastTs;
  sh.v2LastTs = t;
  if (r.nis !== null && sh.nis.length < SHADOW_NIS_SAMPLE_MAX) sh.nis.push(r.nis);
  if (sh.sx === null) {
    sh.sx = r.x; sh.sy = r.y;
    sh.win = [{ t, x: r.x, y: r.y }];
    sh.credAnchor = { x: r.x, y: r.y, t };
    return;
  }
  const step = Math.hypot(r.x - sh.sx, r.y - sh.sy);
  sh.sx = r.x; sh.sy = r.y;
  sh.smoothRawM += step;

  // Stationarity: net displacement of the smoothed track over the sliding
  // window (see the constants' note for why sliding beats an anchor dwell).
  // Prune keeps the newest sample OLDER than the window edge as the boundary
  // reference; until one exists (first 15 s after a reset) the verdict
  // abstains to "moving" — abstention must never suppress credit.
  sh.win.push({ t, x: r.x, y: r.y });
  while (sh.win.length >= 2 && sh.win[1].t <= t - SHADOW_STILL_WINDOW_MS) sh.win.shift();
  // Hysteresis: LEAVING still needs 1.25x the entry ball. A parked phone's
  // slow error drift hovers right at the entry threshold and would otherwise
  // flap out and credit escape hops. 1.25x, not more: a 2 km/h walker nets
  // 8.3 m / 15 s and must clear the escape bar promptly (6.25 m) — synthetics
  // showed 1.5x (7.5 m) re-creating the R-69 under-credit at -11%.
  const entry = Math.max(SHADOW_STILL_NET_M,
                         SHADOW_FLOOR_ACC_K * (Number.isFinite(ac) ? ac : 10));
  const ball = sh.still ? entry * 1.25 : entry;
  const ref = sh.win[0];
  const wasStill = sh.still;
  sh.still = (ref.t <= t - SHADOW_STILL_WINDOW_MS) &&
             Math.hypot(r.x - ref.x, r.y - ref.y) < ball;
  if (sh.still && !wasStill) {
    // Pre-stop residue never escaped the credit floor by construction.
    sh.floorM += sh.pendM;
    sh.pendM = 0;
  } else if (!sh.still && wasStill) {
    // Restart after a stop: credit resumes from HERE. Movement inside the
    // stop window is deliberately forfeited — conservative, and it keeps a
    // drifting parked phone at zero.
    sh.credAnchor = { x: r.x, y: r.y, t };
    sh.pendM = 0;
  }

  if (sh.still) {
    sh.zuptN++;
    sh.k.zupt();
    sh.stillM += step;
    if (prevTs) sh.stillMs += Math.max(0, t - prevTs);
    return;
  }

  // Anchor-quantum crediting: what is credited is the NET anchor-to-anchor
  // hop — a polyline through points spaced ≥ the floor — not the smoothed
  // path between them. The smoothed path still integrates residual wiggle
  // (synthetics put that at +15..+70% on noisy tracks); a net hop's noise
  // bias is ~sigma^2/hop, a few percent, and at ~4 m hops the straight-line
  // corner cut is negligible. pendM (path since anchor) stays as the wiggle
  // telemetry: floorM accrues (path - hop), the shave the polyline took.
  sh.pendM += step;
  const floor = Math.max(SHADOW_FLOOR_M,
                         SHADOW_FLOOR_ACC_K * (Number.isFinite(ac) ? ac : 10));
  const fromCred = Math.hypot(r.x - sh.credAnchor.x, r.y - sh.credAnchor.y);
  if (fromCred >= floor) {
    const dtS = Math.max(0.001, (t - sh.credAnchor.t) / 1000);
    if (fromCred / dtS > SHADOW_MAX_CREDIT_MPS) sh.capM += fromCred;
    else sh.credM += fromCred;
    sh.floorM += Math.max(0, sh.pendM - fromCred);
    sh.pendM = 0;
    sh.credAnchor = { x: r.x, y: r.y, t };
  }
}

// Feed one message's raw fixes into the racer's shadow accumulator. Every
// numeric guard here is load-bearing: fx is client-supplied and must never be
// able to poison the accumulator or throw (the caller runs inside the M-3
// message guard, but a NaN would corrupt silently, not loudly).
function shadowIngest(st, fx) {
  let sh = st.shadow;
  if (!sh) sh = st.shadow = { last: null, rawM: 0, credM: 0, n: 0, firstTs: 0, lastTs: 0,
                              drop: { acc: 0, order: 0, tele: 0, bad: 0 }, over: 0, flushed: false,
                              // v2 pipeline state (inert when SERVER_SHADOW_V2 is false)
                              k: null, sx: null, sy: null, v2LastTs: 0,
                              smoothRawM: 0, stillM: 0, floorM: 0, capM: 0,
                              win: [], still: false, stillMs: 0, zuptN: 0,
                              credAnchor: null, pendM: 0,
                              starvM: 0, starvN: 0, starvSec: 0,
                              nis: [], spd: { lo: 0, hi: 0, na: 0 }, fx: [],
                              seedM: 0, lastFxWallTs: 0 };
  // Phase C seedM: a racer whose accumulator is born mid-race (relay restart,
  // rejoin) already holds credit — fold it in once at creation, exactly as
  // budgetedDistance seeds from baselineDist. Zero on a normal race start.
  if (sh.n === 0) sh.seedM = Math.max(st.authDist || 0, st.baselineDist || 0);
  // Finish snapshot: devices stream fixes for minutes after finishing, so the
  // flushed cred_m includes post-finish walking (22-Aug read: +43–60% "over-
  // credit" that was really cooldown meters). Freeze the race-portion numbers
  // the first time we ingest after st.finished flips; flushShadow publishes
  // them as meta.fin*. One site, fail-soft, never touches crediting itself.
  if (st.finished && !sh.finSnap) {
    sh.finSnap = { credM: sh.credM, rawM: sh.rawM, authM: st.authDist || 0,
                   starvM: sh.starvM, ts: sh.lastTs, wall: Date.now() };
  }
  const list = fx.length > SHADOW_MAX_FX_PER_MSG ? fx.slice(0, SHADOW_MAX_FX_PER_MSG) : fx;
  for (const f of list) {
    if (sh.n >= SHADOW_MAX_FIXES) { sh.over++; continue; }
    if (!Array.isArray(f) || f.length < 4) { sh.drop.bad++; continue; }
    const [t, la, ln, ac, spd] = f;
    if (!Number.isFinite(t) || !Number.isFinite(la) || !Number.isFinite(ln) ||
        la < -90 || la > 90 || ln < -180 || ln > 180) { sh.drop.bad++; continue; }
    sh.n++;
    if (!sh.firstTs) sh.firstTs = t;
    // Rounding keeps a 14400-fix worst case ~600 KB of jsonb; 6 decimals is
    // ~0.1 m, below anything the gates can resolve.
    if (SHADOW_PERSIST_FIXES) {
      sh.fx.push([t, +la.toFixed(6), +ln.toFixed(6),
                  Number.isFinite(ac) ? +ac.toFixed(1) : null,
                  Number.isFinite(spd) ? +spd.toFixed(2) : null]);
    }
    if (Number.isFinite(ac) && ac > SHADOW_MAX_ACC_M) { sh.drop.acc++; continue; }
    if (sh.last) {
      const dtS = (t - sh.last.t) / 1000;
      if (dtS <= 0) { sh.drop.order++; continue; }          // out-of-order / duplicate
      const chord = haversineM(sh.last.la, sh.last.ln, la, ln);
      if (chord / dtS > SHADOW_TELEPORT_MPS) {
        // Teleport: no credit, but RE-ANCHOR — otherwise one glitch fix poisons
        // every subsequent chord against a stale anchor. v2 re-seeds too: the
        // filter would otherwise smoothly interpolate the glitch into length.
        sh.drop.tele++;
        sh.last = { t, la, ln, ac: Number.isFinite(ac) ? ac : null };
        if (SERVER_SHADOW_V2) shadowV2Reset(sh);
        continue;
      }
      sh.rawM += chord;                              // v1 semantics, unchanged
      if (!SERVER_SHADOW_V2) sh.credM += chord;      // v1 mirror when v2 is off
    }
    // v2 sees exactly the fixes raw_m chords are built from (first accepted
    // fix included), so cred-vs-raw deltas are attributable to the gates alone.
    if (SERVER_SHADOW_V2) shadowV2Step(sh, t, la, ln, ac, spd);
    sh.last = { t, la, ln, ac: Number.isFinite(ac) ? ac : null };
    sh.lastTs = t;
    sh.lastFxWallTs = Date.now();   // arrival clock, not fix ts — staleness must
                                    // survive a device clock that lies
  }
}

// ── Phase C ladder (see SHADOW_PHASE_C_DESIGN.md §2) ─────────────────────────
// Rung 1: v2 credit while the fix stream is fresh. Rung 2: today's budgeted
// claim (computed EVERY frame regardless — its st fields feed eviction and the
// fallback must stay warm). Same monotone + target invariants as the budget.
// C0 returns budgetM (dry-run); C1 returns authM. st.authDist tracks the ladder
// in both modes so the flip changes which number the room reads, not the math.
function authoritativeDistance(roomId, room, st, userId, claimed, nowTs) {
  const budgetM = budgetedDistance(room, st, claimed, nowTs);
  const sh = st.shadow;
  const fxFresh = !!(SERVER_SHADOW_V2 && sh && sh.n > 0 &&
                     sh.lastFxWallTs && nowTs - sh.lastFxWallTs <= AUTH_FX_STALE_MS);
  // Starvation rung: when the ladder is live, forfeited gap chords score too —
  // still inside the rawM clamp, so credit can never exceed the raw path.
  // Dry-run (the default) leaves scoring EXACTLY as before; starvM is telemetry.
  const starvM = (SERVER_STARVATION_LADDER && sh) ? sh.starvM : 0;
  let authM = fxFresh ? (sh.seedM + Math.min(sh.credM + starvM, sh.rawM)) : budgetM;
  authM = Math.max(st.authDist || 0, authM);
  const targetM = room.meta && room.meta.targetM;
  if (targetM > 0) authM = Math.min(authM, targetM);
  st.authDist = authM;
  st.budgetDist = budgetM;   // display blend reads this; never used for scoring
  st.authN = (st.authN || 0) + 1;
  if (fxFresh) st.authR1 = (st.authR1 || 0) + 1;
  if (SERVER_AUTH_DRYRUN && (!st.authLogTs || nowTs - st.authLogTs >= 60000)) {
    st.authLogTs = nowTs;
    log(`AUTH-DELTA room=${roomId} user=${userId} rung=${fxFresh ? 1 : 2} ` +
        `auth=${Math.round(authM)} budget=${Math.round(budgetM)} drift=${Math.round(authM - budgetM)} ` +
        `r1=${st.authR1 || 0}/${st.authN}` +
        (sh && sh.starvM ? ` starv=${Math.round(sh.starvM)}/${sh.starvN}${SERVER_STARVATION_LADDER ? '(live)' : '(dry)'}` : ''));
  }
  return SERVER_AUTHORITATIVE_DISTANCE ? authM : budgetM;
}

// Display blend (see the SERVER_DISPLAY_BLEND constants): the distance the room
// RENDERS, given the distance the room SCORES. Pure — extracted by the shadow
// harness. Identity when the blend is off, when authoritative mode is off (distM
// already IS the budget then), or when there is no budget to lead with.
function displayDistance(room, st, authM) {
  if (!SERVER_DISPLAY_BLEND || !SERVER_AUTHORITATIVE_DISTANCE) return authM;
  const budgetM = st && st.budgetDist;
  if (!(budgetM > authM)) return authM;                      // auth leads or equal: truth wins
  let d = Math.max(authM, Math.min(budgetM, authM + DISPLAY_LEAD_M));
  const targetM = room && room.meta && room.meta.targetM;
  // Never show the line crossed before the server witnesses it: while auth is
  // short of target the display parks just under it, and the moment the real
  // CROSSING runs auth to target the clamp vanishes (authM >= targetM path
  // never reaches here — budget cannot exceed an auth already at target-cap).
  if (targetM > 0 && authM < targetM) d = Math.min(d, targetM - DISPLAY_FINISH_HOLD_M);
  return d;
}

// R-107 predicate: does this racer's first witnessed self-finish diverge from
// the authoritative ladder? Pure — extracted so the shadow harness can drive it.
// Abstains (false) without meta, without enough observed ladder frames, or when
// the guard/mode flags are off. A server CROSSING runs authDist to target before
// any honest claim, and rung 2 IS the budgeted claim, so neither path can trip it.
function finishClaimDivergent(room, st) {
  if (!FINISH_CLAIM_GUARD || !SERVER_AUTHORITATIVE_DISTANCE) return false;
  const targetM = room && room.meta && room.meta.targetM;
  if (!(targetM > 0)) return false;
  if ((st.authN || 0) < FINISH_CLAIM_MIN_FRAMES) return false;
  return (st.authDist || 0) < targetM * FINISH_CLAIM_MIN_FRAC;
}

// R-107: stamp a divergent self-finished result row flagged. The client writes
// its ranked row on its own schedule, so the row may not exist when the finish
// relay arrives — retry on a ladder that outlives the client's worst-case save
// path (FINISH_NOROW_DWELL_MS). First-write-wins with the anti-cheat flags: a
// row some gate already flagged keeps its original reason. Fire-and-forget,
// never throws, DB-only — safe against a torn-down room.
function flagDivergentFinish(roomId, userId, authM, targetM, reason = 'server_credit_low', attempt = 0) {
  if (!supabase) return;
  const RETRY_MS = [15000, 45000, 120000];
  // R-107 hardening: try the DEMOTE first — service-role RPC that unranks the
  // row, surrenders podium credit and reranks survivors, but REFUSES when a
  // witnessed live crossing exists for this racer ('witnessed'). 'no_ranked_row'
  // means the claim has not landed (or landed unranked) yet — fall through to
  // the flag update, whose retry ladder outlives the client's worst-case save
  // path exactly as before. Any RPC failure fail-softs to flag-only, so the
  // pre-hardening behavior is the floor, never less.
  const flagOnly = () => supabase.from('race_results')
    .update({ flagged: true, flag_reason: reason })
    .eq('room_id', roomId).eq('user_id', userId)
    .or('flagged.is.null,flagged.eq.false')
    .select('id')
    .then(({ data, error }) => {
      if (error) { log('FINISH-DIVERGENT flag write failed', roomId, userId, error.message); return; }
      if (data && data.length) {
        log(`FINISH-DIVERGENT-FLAGGED room=${roomId} user=${userId} auth=${Math.round(authM)} target=${Math.round(targetM)} reason=${reason} attempt=${attempt}`);
        return;
      }
      // No row matched: either not written yet (retry) or already flagged (a
      // retry is then a harmless no-op that exhausts quietly).
      if (attempt < RETRY_MS.length) {
        setTimeout(() => flagDivergentFinish(roomId, userId, authM, targetM, reason, attempt + 1), RETRY_MS[attempt]);
      } else {
        log(`FINISH-DIVERGENT-UNFLAGGED room=${roomId} user=${userId} (no unflagged row after ${attempt} retries)`);
      }
    }, (e) => log('FINISH-DIVERGENT flag write threw', roomId, userId, e && e.message));
  if (!FINISH_CLAIM_DEMOTE) { flagOnly(); return; }
  supabase.rpc('demote_unwitnessed_finish', { p_room_id: roomId, p_user_id: userId, p_reason: reason })
    .then(({ data: verdict, error }) => {
      if (error) {
        log('FINISH-DIVERGENT demote rpc failed — falling back to flag', roomId, userId, error.message);
        flagOnly();
        return;
      }
      if (verdict === 'demoted') {
        log(`FINISH-DIVERGENT-DEMOTED room=${roomId} user=${userId} auth=${Math.round(authM)} target=${Math.round(targetM)} reason=${reason} attempt=${attempt}`);
        return;
      }
      if (verdict === 'witnessed') {
        log(`FINISH-DIVERGENT-WITNESSED room=${roomId} user=${userId} (crossing exists — not demoted)`);
        return;
      }
      // 'no_ranked_row' (or anything unexpected): flag the unranked row / retry
      // until the claim lands, same contract as before.
      flagOnly();
    }, (e) => { log('FINISH-DIVERGENT demote rpc threw', roomId, userId, e && e.message); flagOnly(); });
}

// Persist every racer's shadow tally once. Multiple hooks may race (terminal
// settle, ROOM CLOSED, GC) — the per-racer flag plus the table's PK upsert make
// that harmless. Fire-and-forget: shadow must never delay teardown paths.
function flushShadow(roomId, room, via) {
  if (!SERVER_SHADOW_DISTANCE || !supabase || !room || !room.racers) return;
  for (const [userId, st] of room.racers) {
    const sh = st.shadow;
    if (!sh || sh.flushed || sh.n === 0) continue;
    sh.flushed = true;
    const meta = { firstTs: sh.firstTs, lastTs: sh.lastTs, over: sh.over, via };
    if (SERVER_SHADOW_V2) {
      // NIS percentiles (client census semantics): filter-health readout —
      // p50 far above 2.0 means stated accuracy under-states the true error.
      const sorted = sh.nis.slice().sort((a, b) => a - b);
      const pct = (p) => sorted.length
        ? Math.round(sorted[Math.min(sorted.length - 1, Math.max(0, Math.round((sorted.length - 1) * p)))] * 10) / 10
        : null;
      Object.assign(meta, {
        v: 2,
        smoothRawM: Math.round(sh.smoothRawM),  // smoothed track pre-gating (the pure-Kalman number)
        stillM: Math.round(sh.stillM),          // suppressed while stationary
        floorM: Math.round(sh.floorM),          // sub-floor residue, never released
        capM: Math.round(sh.capM),              // dropped by the 12 m/s cap
        stillSec: Math.round(sh.stillMs / 1000),
        zupt: sh.zuptN,
        nisP50: pct(0.5), nisP90: pct(0.9),
        nisMax: sorted.length ? Math.round(sorted[sorted.length - 1] * 10) / 10 : null,
        spd: sh.spd,                            // Doppler lo/hi/na buckets — telemetry only
      });
      // Starvation rung readout: metres the gap-chord rung banked, how many
      // gaps, seconds of race those gaps covered, and whether it SCORED.
      // The dry-run go/no-go reads: cred_m + starv.m vs walked truth on a
      // battery-saver race, and starv.m ≈ 0 on every healthy/parked race.
      if (sh.starvN) {
        meta.starv = { m: Math.round(sh.starvM), n: sh.starvN,
                       sec: sh.starvSec, live: SERVER_STARVATION_LADDER };
      }
      if (sh.finSnap && sh.finSnap.starvM) meta.finStarvM = Math.round(sh.finSnap.starvM);
      // cred≤raw is a published invariant (register R-100 col 15). The CV
      // filter can overshoot through speed changes and mint smoothed length
      // beyond the raw path (LG 2f314303: smooth 611 / raw 598 / cred 605), so
      // enforce it here and keep the excess visible for tuning.
      if (sh.credM > sh.rawM) {
        meta.overRawM = Math.round(sh.credM - sh.rawM);
        sh.credM = sh.rawM;
      }
      // Finish snapshot (see shadowIngest): credit at the moment the racer
      // finished, before post-finish cooldown fixes kept accruing. Filled here
      // when the racer finished but no gps message arrived afterwards (flush
      // values ARE the finish values then). finM carries the cred≤raw clamp.
      if (st.finished && !sh.finSnap) {
        sh.finSnap = { credM: sh.credM, rawM: sh.rawM, authM: st.authDist || 0,
                       starvM: sh.starvM, ts: sh.lastTs, wall: Date.now() };
      }
      if (sh.finSnap) {
        meta.finM    = Math.round(Math.min(sh.finSnap.credM, sh.finSnap.rawM));
        meta.finRawM = Math.round(sh.finSnap.rawM);
        meta.finAuthM = Math.round(sh.finSnap.authM);
        meta.finTs   = sh.finSnap.ts;
      }
      // Phase C0 per-race summary: final ladder value, rung-1 frame share, seed.
      if (SERVER_AUTH_DRYRUN && st.authN) {
        meta.auth = { m: Math.round(st.authDist || 0), n: st.authN,
                      r1: st.authR1 || 0, seedM: Math.round(sh.seedM || 0),
                      live: SERVER_AUTHORITATIVE_DISTANCE };
      }
    }
    const row = {
      room_id: roomId, user_id: userId,
      raw_m: Math.round(sh.rawM), cred_m: Math.round(sh.credM),
      client_m: Math.round(st.lastDist || 0),
      fixes: sh.n, drops: sh.drop,
      meta,
    };
    log(`SHADOW room=${roomId} user=${userId} raw=${row.raw_m} cred=${row.cred_m} ` +
        `client=${row.client_m} n=${sh.n} drops=${JSON.stringify(sh.drop)} via=${via}` +
        (SERVER_SHADOW_V2
          ? ` v2 smooth=${meta.smoothRawM} still=${meta.stillM} floor=${meta.floorM} cap=${meta.capM} stillSec=${meta.stillSec} fin=${meta.finM != null ? meta.finM : '-'}` +
            (meta.starv ? ` starv=${meta.starv.m}/${meta.starv.n}(${meta.starv.live ? 'live' : 'dry'})` : '')
          : ''));
    // ignoreDuplicates: first successful flush wins (terminal fires before
    // room_closed/gc), so a late flush can never overwrite the canonical row.
    supabase.from('race_shadow_distance').upsert(row, { onConflict: 'room_id,user_id', ignoreDuplicates: true })
      .then(({ error }) => { if (error) log('SHADOW upsert failed', roomId, userId, error.message); })
      .catch((e) => log('SHADOW upsert error', roomId, userId, e && e.message));
    if (SHADOW_PERSIST_FIXES && sh.fx && sh.fx.length) {
      supabase.from('race_shadow_fixes')
        .upsert({ room_id: roomId, user_id: userId, fixes: sh.fx, n: sh.fx.length, meta: { via } },
                { onConflict: 'room_id,user_id', ignoreDuplicates: true })
        .then(({ error }) => { if (error) log('SHADOW-FIX upsert failed', roomId, userId, error.message); })
        .catch((e) => log('SHADOW-FIX upsert error', roomId, userId, e && e.message));
    }
  }
}

// Canonical replay: one sample per REPLAY_STEP_MS of the racer's ACCEPTED
// (budget-trimmed) distance, positionally indexed from started_at so every
// racer's curve shares one timeline. Silent stretches (locked device, dropped
// socket) backfill with the last value — monotonic by construction, and a
// late-arriving burst can only raise a sample, never lower it. Same abstention
// as recordCrossing: no started_at anchor, no samples.
function recordReplaySample(room, st, accepted) {
  if (!SERVER_REPLAY_CURVE) return;
  // Post-terminal gps (a device loitering on results, or rejoining after the
  // race ended) must not seed a fresh curve: a new racer state backfills from 0
  // and the gc flush would clobber the canonical terminal curve with it.
  if (room.terminal) return;
  const startedAt = room.meta && room.meta.startedAt ? new Date(room.meta.startedAt).getTime() : null;
  if (!startedAt) return;
  const idx = Math.floor((Date.now() - startedAt) / REPLAY_STEP_MS);
  if (idx < 0 || idx >= REPLAY_MAX_SAMPLES) return;
  if (!st.replayCurve) st.replayCurve = { arr: [], flushed: false };
  const arr = st.replayCurve.arr;
  const m = Math.max(0, Math.round(accepted));
  const prev = arr.length ? arr[arr.length - 1] : 0;
  while (arr.length <= idx) arr.push(prev);
  if (m > arr[idx]) arr[idx] = m;
}

// Flush at room terminal — same contract and call sites as flushShadow:
// fire-and-forget, idempotent via the flushed flag, a lost write costs only
// this room's canonical replay (clients fall back to their local frames).
function flushReplayCurves(roomId, room, via) {
  if (!SERVER_REPLAY_CURVE || !supabase || !room || !room.racers) return;
  const startedAt = room.meta && room.meta.startedAt;
  if (!startedAt) return;
  for (const [userId, st] of room.racers) {
    const rc = st.replayCurve;
    if (!rc || rc.flushed || rc.arr.length < 2) continue;
    rc.flushed = true;
    // ignoreDuplicates: first successful flush wins (terminal fires before
    // room_closed/gc), so a late flush can never overwrite the canonical curve.
    supabase.from('race_replay_curves').upsert({
      room_id: roomId, user_id: userId,
      t0: startedAt, step_ms: REPLAY_STEP_MS, dist_m: rc.arr,
    }, { onConflict: 'room_id,user_id', ignoreDuplicates: true }).then(({ error }) => {
      if (error) log('REPLAY-FLUSH upsert failed', roomId, userId, error.message);
      else log(`REPLAY-FLUSH room=${roomId} user=${userId} samples=${rc.arr.length} via=${via}`);
    }, (e) => log('REPLAY-FLUSH error', roomId, userId, e && e.message));
  }
}

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

// ── R-61 helpers ─────────────────────────────────────────────────────────────
//
// Hydrates room.meta with what the budget needs. Fire-and-forget on purpose: the
// gps handler must never await, so the FIRST few frames of a race are evaluated
// with no meta and are therefore admitted untrimmed. That is the correct trade —
// this is a cheat mitigation, not an authorisation gate, and the alternative
// (blocking the hot path on a network read) would cost every honest racer.
//
// `activity_type` is new to this read; the other two fields match the two existing
// lazy hydrations, so a room already hydrated by one of those is re-read once to
// pick the activity up. Guarded by metaFetching so a burst of frames triggers one
// request, not one per frame.
async function ensureBudgetMeta(roomId, room) {
  if (!supabase || room.metaFetching) return;
  // 🔑 started_at IS NULL UNTIL THE RACE STARTS, AND THIS CACHE USED TO KEEP THAT
  // NULL FOREVER. Frames flow before the start (gps_ready in the lobby, and any
  // window where the start write is slow), so the first fetch can legitimately
  // land on a not-yet-started room. Caching that made room.meta.startedAt null
  // for the whole race, which silently disabled recordCrossing's anchor — found
  // on device 2026-08-14, room EMU_R64_3: racer3 finished 500 m and NO crossing
  // row was written, because the meta had been cached during a stalled start.
  // Keep re-reading while the anchor is missing, at most every 10s so a lobby
  // sitting at 1 Hz does not turn into a request per frame.
  const haveAnchor = room.meta && room.meta.activity !== undefined && room.meta.startedAt;
  if (haveAnchor) return;
  if (room.meta && room.meta.activity !== undefined) {
    const now = Date.now();
    if (room.metaRetryAt && now < room.metaRetryAt) return;
    room.metaRetryAt = now + 10000;
  }
  room.metaFetching = true;
  try {
    const { data: r } = await supabase
      .from('race_rooms').select('started_at, target_distance_m, activity_type')
      .eq('id', roomId).single();
    if (r) {
      room.meta = { startedAt: r.started_at, targetM: r.target_distance_m,
                    activity: r.activity_type || null };
    }
  } catch (e) { log('BUDGET-META error', roomId, e && e.message); }
  finally { room.metaFetching = false; }
}

// Returns the distance this racer may be credited with, given what they claim.
// Pure apart from the two fields it advances on `st`, so the policy above can be
// reasoned about (and changed) without touching the message handler.
function budgetedDistance(room, st, claimed, now) {
  const raw = (typeof claimed === 'number' && Number.isFinite(claimed) && claimed >= 0)
    ? claimed : (st.acceptedDist || 0);

  if (!SERVER_DISTANCE_BUDGET) return raw;

  // Anchor the clock the first time we see this racer. Preferring the room's start
  // over `now` is what stops the very first frame being a free teleport: a racer
  // whose first frame lands 4 minutes into the race gets 4 minutes of budget, not
  // an unbounded seed. A racer reconnecting keeps whatever they had already earned
  // (st.acceptedDist survives on the room state, and the DB baseline seeds it on
  // rejoin), so a reconnect is not a fresh start either.
  if (st.acceptedTs == null) {
    const startedAt = room.meta && room.meta.startedAt
      ? new Date(room.meta.startedAt).getTime() : null;
    st.acceptedTs = (startedAt && startedAt <= now) ? startedAt : now;
    st.acceptedDist = Math.max(st.acceptedDist || 0, st.lastDist || 0, st.baselineDist || 0);
  }

  // ABSTAIN: the activity is not known yet (hydration in flight, or the read
  // failed). Trimming on the unknown-room default would apply a cycling ceiling to
  // a walking race, which is meaningless, so admit and let hydration catch up.
  const activity = room.meta && room.meta.activity;
  const capKmh = activity ? (BUDGET_SPEED_CAPS_KMH[activity] || BUDGET_UNKNOWN_CAP_KMH)
                          : BUDGET_UNKNOWN_CAP_KMH;

  const dtMs = Math.max(0, now - st.acceptedTs);
  const allowanceM = (capKmh * BUDGET_MARGIN) * (dtMs / 3600000) * 1000;
  const ceiling = (st.acceptedDist || 0) + Math.max(BUDGET_FLOOR_M, allowanceM);

  // Monotone by construction: max() against the distance already granted, so a
  // client that regresses (or a trim) can never push a racer backwards into the
  // stationary-eviction path.
  let accepted = Math.max(st.acceptedDist || 0, Math.min(raw, ceiling));

  // The race cannot be longer than the race. Clamping here mirrors
  // save_race_result's own clamp, so the number peers see, the number the eviction
  // logic reads and the number the finish stores cannot disagree.
  const targetM = room.meta && room.meta.targetM;
  if (targetM > 0) accepted = Math.min(accepted, targetM);

  if (raw > accepted + 1) {
    st.trimmed = (st.trimmed || 0) + 1;
    st.trimmedM = Math.round((st.trimmedM || 0) + (raw - accepted));
    // One line per trim is too noisy at GPS cadence on a flapping connection; log
    // the first, then every 20th, which is enough to see a sustained forgery.
    if (st.trimmed === 1 || st.trimmed % 20 === 0) {
      log(`BUDGET-TRIM room=${room.id || '?'} claimed=${Math.round(raw)} accepted=${Math.round(accepted)} ` +
          `dt=${dtMs}ms cap=${capKmh}km/h act=${activity || 'unknown'} n=${st.trimmed}`);
    }
  }

  st.acceptedDist = accepted;
  st.acceptedTs = now;
  return accepted;
}

// R-64 Part 3, phase 1. Record the instant the ACCEPTED distance first reached the
// target — the thing finish_time_ms was supposed to be and is not.
//
// 🔑 THIS RELAY CAN SEE THE CROSSING THE STALLED CLIENT CANNOT REPORT. That is the
// whole basis of Part 3 and it is measured, not assumed: 2026-08-14 room 194d825e,
// peers rendered tina_p at 0.50 km from 11:50:46 while her finish row did not land
// until 11:52:54. The frames kept arriving here throughout — the seed hang blocks
// the FINISH path, not the GPS path.
//
// Deliberately NOT a promise anyone awaits, and it swallows its own errors: a
// measurement must never cost a frame on the 1 Hz hot path, and must never be able
// to fail a race. If the write is lost, the measurement is lost — nothing else.
function recordCrossing(roomId, room, st, userId, accepted) {
  if (!SERVER_CROSSING_STAMP || !supabase) return;
  if (st.crossingStamped) return;
  const targetM = room.meta && room.meta.targetM;
  // ABSTAIN while the target is unknown (hydration in flight) — same rule the
  // budget follows. A crossing cannot happen before hydration on any real race.
  if (!(targetM > 0) || !(accepted >= targetM)) return;
  const startedAt = room.meta.startedAt ? new Date(room.meta.startedAt).getTime() : null;
  if (!startedAt) return;                 // no anchor, no comparable elapsed
  st.crossingStamped = true;              // set BEFORE the await: at 1 Hz the next
                                          // frame arrives long before the insert.
  const now = Date.now();
  // baseline: this racer was already over the line on their first frame of this
  // connection, so the real crossing happened where we could not see it. Marked,
  // never silently treated as a crossing — correcting a rank on one would hand a
  // place to whoever's socket survived, which is the opposite of the fix.
  const source = (st.baselineDist || 0) >= targetM ? 'baseline' : 'live';
  const elapsed = Math.max(0, now - startedAt);
  log(`CROSSING room=${roomId} user=${userId} elapsed=${elapsed}ms ` +
      `accepted=${Math.round(accepted)} target=${targetM} source=${source}`);
  supabase.from('race_crossings').insert({
    room_id: roomId, user_id: userId,
    crossed_at: new Date(now).toISOString(),
    crossed_elapsed_ms: elapsed,
    accepted_m: Math.round(accepted),
    target_m: targetM,
    source,
  }).then(({ error }) => {
    // 23505 = the earliest observation already landed, which is the intended
    // outcome of a reconnect and not a problem worth a line.
    if (error && error.code !== '23505') log('CROSSING write failed', roomId, error.message);
  }, (e) => log('CROSSING write threw', roomId, e && e.message));

  // Crossing-resolve: write the ranked finish NOW, server-side. Same contract as
  // the stamp above — fire-and-forget, swallows its own errors, never costs a
  // frame and never fails a race; a lost write degrades to the pre-fix behaviour
  // (the racer's own claim resolves them at unlock).
  if (SERVER_CROSSING_RESOLVE && source === 'live') {
    supabase.rpc('resolve_crossing_finish', {
      p_room_id: roomId, p_user_id: userId,
      p_elapsed_ms: elapsed, p_distance_m: Math.round(accepted),
    }).then(({ data: pos, error }) => {
      if (error) { log('CROSSING-RESOLVE rpc failed', roomId, userId, error.message); return; }
      // null = room not 'racing', target unmet, or a standing gate verdict
      // (mock/T8/DNF row) this resolve must not overturn — the claim path owns it.
      if (pos == null) return;
      log(`CROSSING-RESOLVE room=${roomId} user=${userId} pos=${pos}`);
      // Mirror the client's own 'finished' relay exactly: first-write-wins on the
      // terminal flags (E-12), finishedAt stamps the FIRST witness only, the
      // broadcast flips peer HUDs off "finishing…", and the completion check can
      // now settle the room with no client awake to claim.
      if (st && !st.finished && !st.quit && !st.evicted) {
        st.finishedAt = Date.now();
        st.finished = true;
      }
      room.hasFinisher = true;
      broadcast(room, { event: 'finished', payload: {
        user_id: userId, position: pos, distance_m: Math.round(accepted),
      } });
      scheduleCompletionCheck(roomId, room);
      void (async () => {
        await markLifecycle(roomId, userId, 'finished', 'crossing_resolve');
        scheduleCompletionCheck(roomId, room);
      })();
    }, (e) => log('CROSSING-RESOLVE rpc threw', roomId, e && e.message));
  }
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
      supabase.from('race_results').select('user_id, finish_position, finish_time_ms, distance_covered_m').eq('room_id', roomId),
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
    // R-116: an UNRANKED row whose racer completed the distance (finish_time_ms
    // set + distance at target — a quit row never reaches target) is still a
    // completer waiting on the stragglers. Register rooms 90d25427/edf04fe0 sat
    // 'racing' for hours: every finisher was mock-flagged unranked, so
    // hasFinisher never unlocked the gone window below and the room outlived
    // relay memory. Client parity: checkRaceComplete's quitCount already counts
    // these rows toward completion. targetM 0 (failed meta read) disables the
    // term — fail-closed to today's behavior.
    const metaTargetM = room.meta?.targetM || 0;
    const hasCompleter = hasFinisher || (results || []).some(r =>
      r.finish_position == null && r.finish_time_ms != null &&
      metaTargetM > 0 && (r.distance_covered_m || 0) >= metaTargetM);
    const now = Date.now();
    const unresolved = members.filter(({ user_id }) => {
      if (settled.has(user_id)) return false;
      // The 90s-gone shortcut only applies when a COMPLETER is waiting on the
      // straggler (client-parity: only a present finisher/driver ever completed
      // on the gone window). Without one, nothing but a durable result row
      // (quit/eviction) resolves a racer — a zero-completer room can never be
      // settled out from under a screen-locked racer who is still racing.
      if (!hasCompleter) return true;
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
    } else if (hasCompleter) {
      // R-116: unranked completer(s) only. Request 'finished' and let
      // settle_room decide — its C5 gate keeps the outcome honest (witnessed
      // live crossing → 'finished', 6-min orphan-crossing deferral → the
      // reconcile cron may still rank the row, otherwise downgrade to
      // 'cancelled'). No new terminal semantics live here; a 'deferred' return
      // fails settleWon and the room settles on a later check or the 15-min
      // cron rule.
      const won = await settleRoom(roomId, 'finished', 'all_done');
      if (won === null) { log('SERVER-COMPLETE (unranked completer) failed', roomId); return; }
      if (settleWon(won)) {
        log(`SERVER-COMPLETE unranked_completer room=${roomId}${allMarked ? ' all_marked' : ''} settle=${won}`);
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
             evicted: false, goneNotified: false, evicting: false, warnChecking: false,
             // R-49: last moment ANY gps frame arrived from this racer on this
             // socket, and whether the current silent episode has been logged.
             lastDataTs: Date.now(), gapLogged: false,
             // R-61 budget. acceptedTs stays null until the first frame so the
             // anchor can be the race start rather than the moment of joining —
             // seeding it here would hand a late joiner a full-race allowance in
             // one frame.
             acceptedDist: 0, acceptedTs: null, trimmed: 0, trimmedM: 0 };
      room.racers.set(userId, st);
    }
    st.connected = true; st.disconnectedAt = 0; st.goneNotified = false;
    // R-49: a JOIN is data. Without this, a reconnect after a long freeze would
    // immediately re-log a gap that the reconnect itself has ended.
    st.lastDataTs = Date.now(); st.gapLogged = false;
    // The stationary clock deliberately SURVIVES a reconnect. It used to be
    // re-seeded here, which handed any racer whose transport flapped a fresh
    // 10 minutes on every rejoin: observed 2026-08-08 in room 78113077, where an
    // iOS racer stood still for 12m26s and was never evicted because a 1006 drop
    // at 06:08:54 and a rejoin at 06:09:38 restarted the clock 6 minutes in.
    // Preserving it is safe in the other direction too — absence of frames is not
    // evidence of movement, and the first frame at/above STATIONARY_KMH resets
    // the clock immediately, so a racer who really was running is never evicted
    // for the gap. A racer who is genuinely gone is governed by REQ1's
    // disconnect clock during that same window.
    if (fresh) st.lastMoveTs = Date.now();
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
        // ── R-61: the distance the room sees is the SERVER's, not the client's ──
        // Everything downstream — the peer fanout, the eviction progress test, the
        // quit/DNF distance and the finish-time re-read — reads from here, so this
        // single substitution is what makes the budget authoritative rather than
        // advisory. Racers with no state row (should not happen on a live race)
        // fall through to the claimed value, exactly as before.
        const nowTs = Date.now();
        // R-49: any gps frame ends a silent episode. Log the resume side of the
        // pair BEFORE refreshing the stamp, so the episode length is the truth.
        if (st) {
          if (st.gapLogged) {
            log(`PRESENCE-RESUME room=${roomId} user=${ws.userId} gap_ms=${nowTs - st.lastDataTs}`);
            st.gapLogged = false;
          }
          st.lastDataTs = nowTs;
        }
        // Phase 3 shadow: score the piggybacked raw fixes. Read-only w.r.t.
        // everything below — the room continues to run on the client's number.
        // Post-terminal fixes carry no information about the race (R-90's
        // sibling: a racer loitering after finishing inflated raw_m 600→5797
        // and the gc flush overwrote the clean terminal row).
        if (SERVER_SHADOW_DISTANCE && st && !room.terminal && Array.isArray(payload.fx) && payload.fx.length) {
          shadowIngest(st, payload.fx);
        }
        if (SERVER_DISTANCE_BUDGET) void ensureBudgetMeta(roomId, room);
        const distM = st ? authoritativeDistance(roomId, room, st, ws.userId, payload.distance_m, nowTs)
                         : payload.distance_m;
        // R-64 Part 3 phase 1 — measurement only, changes nothing below it.
        if (st) recordCrossing(roomId, room, st, ws.userId, distM);
        // The two RENDER consumers — the live fan-out and the replay recorder —
        // get the display blend; every scoring consumer keeps distM (auth).
        // Replay deliberately matches what the screens showed (2026-08-22
        // preview of c200d981: curves on pure auth replayed the realme's
        // still-clamp lag that the live blend now hides).
        const dispM = st ? displayDistance(room, st, distM) : distM;
        if (st) recordReplaySample(room, st, dispM);
        room.gps.set(ws.userId, {
          user_id: ws.userId,
          distance_m: dispM,
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
          // R-61: judged on the ACCEPTED distance, not the claimed one — otherwise a
          // forged frame would still reset the stationary clock and a cheat could
          // sit out an entire race without ever being evicted. The speed term is
          // deliberately left reading the client's field: it is the honest racer's
          // protection against a trim stalling their progress, and on its own it can
          // only ever KEEP someone in a race, never advance their position.
          const d = typeof distM === 'number' ? distM : 0;
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
          // E-12: terminal state is first-write-wins. This was the only writer of
          // the three terminal flags with no guard — hydrateRacerFromDb and
          // evictRacer both check first — so a 'finished' relay followed by a
          // 'racer_quit' left st.finished and st.quit BOTH true, the one thing
          // every reader below assumes is exclusive.
          // Not a new rule: mark_member_lifecycle already refuses exactly this
          // downgrade in the DB (v_cur='finished' -> 'noop'), and that column is
          // what hydrateRacerFromDb rebuilds from after a restart. This aligns the
          // in-memory cache with the authority it is a cache OF.
          // A repeat 'finished' relay is unaffected: it was already idempotent via
          // the finishedAt check below, and st.finished is already true.
          if (st && !st.finished && !st.quit && !st.evicted) {
            if (event === 'finished') {
              // finishedAt stamps the FIRST witnessed finish only, so the dwell window
              // cannot be reset by a repeat relay (handleFinish resends on a landed
              // retry, and raceSendReliable itself retries).
              if (!st.finished) st.finishedAt = Date.now();
              st.finished = true;
              // R-107: a self-finish claim while the authoritative ladder holds a
              // fraction of the target is a divergent result — the client's own
              // ranked write is about to (or already did) land a position the
              // server never witnessed. Detect on the first witnessed finish only;
              // abstain without meta, without enough ladder frames, or when a
              // CROSSING already ran authDist up to target (the honest path).
              // no_server_fixes (R-107 hardening): the ladder observed enough
              // gps frames to speak, yet the shadow accumulator never ingested
              // a single fix. Rung 2 tracks the budgeted claim, so auth stays
              // healthy and finishClaimDivergent cannot see this — but an
              // unmodified client always piggybacks fx on the gps message, so
              // frames-without-fixes is positive evidence on its own. Kept at
              // the call site (not in the pure predicate) so the harness
              // contract of finishClaimDivergent is unchanged.
              const noFixes = FINISH_CLAIM_GUARD && SERVER_AUTHORITATIVE_DISTANCE &&
                room.meta && room.meta.targetM > 0 &&
                (st.authN || 0) >= FINISH_CLAIM_MIN_FRAMES &&
                (!st.shadow || (st.shadow.n || 0) === 0);
              if (!st.claimDivergent && (finishClaimDivergent(room, st) || noFixes)) {
                st.claimDivergent = true;
                const tgtM = room.meta.targetM;
                const reason = finishClaimDivergent(room, st) ? 'server_credit_low' : 'no_server_fixes';
                log(`FINISH-DIVERGENT room=${roomId} user=${ws.userId} ` +
                    `auth=${Math.round(st.authDist || 0)} target=${Math.round(tgtM)} ` +
                    `frames=${st.authN} r1=${st.authR1 || 0} reason=${reason}`);
                flagDivergentFinish(roomId, ws.userId, st.authDist || 0, tgtM, reason);
              }
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
        flushShadow(roomId, room, 'room_closed');
        flushReplayCurves(roomId, room, 'room_closed');
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

// ── R-49: presence fan-out ───────────────────────────────────────────────────
// Deliberately its OWN timer, not a rider on the gps batch: the batch is gated
// on room.dirty, so a fully frozen room sends nothing — which is precisely the
// moment presence must still flow. One small message per active room per 5 s.
// gap_ms is measured on THIS server's clock (immune to any client freeze).
// Terminal/evicted/quit/finished racers are included with their flags so the
// consuming client never has to guess why someone went silent.
const presenceTimer = setInterval(() => {
  if (!SERVER_PRESENCE) return;
  // M-3: contain a throw so one bad room can't kill every room's presence.
  try {
    const now = Date.now();
    for (const [roomId, room] of rooms) {
      if (room.terminal || !room.raceActive || room.clients.size === 0) continue;
      if (room.racers.size === 0) continue;
      const racers = [];
      for (const [uid, st] of room.racers) {
        racers.push({
          id: uid,
          conn: !!st.connected,
          gap_ms: st.connected ? Math.max(0, now - (st.lastDataTs || now)) : null,
          fin: !!st.finished, quit: !!st.quit, evict: !!st.evicted,
        });
        // Device-test readout: one PRESENCE-GAP line per silent episode of a
        // CONNECTED racer (the disconnected case already logs LEAVE + the REQ1
        // clock). The matching PRESENCE-RESUME is logged by the gps handler.
        if (st.connected && !st.finished && !st.quit && !st.evicted && !st.gapLogged
            && st.lastDataTs && now - st.lastDataTs >= PRESENCE_GAP_LOG_MS) {
          st.gapLogged = true;
          log(`PRESENCE-GAP room=${roomId} user=${uid} gap_ms=${now - st.lastDataTs}`);
        }
      }
      broadcast(room, { event: 'presence', payload: { racers, ts: now } });
    }
  } catch (e) {
    log(`SWEEP error timer=presence err=${e && e.message}`);
    if (e && e.stack) console.error(e.stack);
    captureServer(e, { guard: 'sweep', timer: 'presence' });   // D-08
  }
}, PRESENCE_INTERVAL_MS);

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

// LiveKit voice sweep — see the constant block for why this lives on the relay.
// Fire-and-forget with a log line either way; a failed cycle just waits for the
// next one. Skipped in TEST_MODE (no Supabase env).
const livekitSweepTimer = (LIVEKIT_SWEEP_ENABLED && !TEST_MODE) ? setInterval(async () => {
  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/livekit-sweep`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        'Content-Type': 'application/json',
      },
      body: '{}',
      signal: AbortSignal.timeout(30000),
    });
    const body = await res.json().catch(() => null);
    if (!res.ok) log('LIVEKIT-SWEEP failed', res.status, body && body.error);
    else if (body && body.closed && body.closed.length) log(`LIVEKIT-SWEEP closed=${body.closed.join(',')} live=${body.live}`);
  } catch (e) {
    log('LIVEKIT-SWEEP error', e && e.message);
  }
}, LIVEKIT_SWEEP_INTERVAL_MS) : null;
if (livekitSweepTimer && livekitSweepTimer.unref) livekitSweepTimer.unref();

const inactivityTimer = setInterval(() => {
  // M-3: contain a throw so one bad room can't kill the whole lifecycle sweep
  // (and with it every room's eviction/completion timing).
  try {
    const now = Date.now();
    for (const [roomId, room] of rooms) {
      if (room.clients.size === 0 && room.emptySince &&
          (room.terminal || now - room.emptySince > EMPTY_ROOM_TTL_MS)) {
        if (room.completeTimer) clearTimeout(room.completeTimer);
        flushShadow(roomId, room, 'gc');
        flushReplayCurves(roomId, room, 'gc');
        rooms.delete(roomId);
        log(`ROOM GC room=${roomId}`);
        continue;
      }
      // Spectator eviction on terminal rooms (see the constant block). Acked
      // spectators close after the linger; unacked ones on the hard deadline.
      // close() feeds the normal LEAVE path, so the last socket leaving still
      // triggers ROOM CLOSED + its flushes exactly as a voluntary exit would.
      if (room.terminal && room.terminal.at && room.clients.size > 0) {
        const age = now - room.terminal.at;
        if (age > SPECTATOR_TERMINAL_LINGER_MS) {
          for (const client of room.clients) {
            if (client.role !== 'spectator' || client.readyState !== WebSocket.OPEN) continue;
            const acked = client.ackedTerminalMid === room.terminal.mid;
            if (!acked && age <= SPECTATOR_TERMINAL_HARD_CLOSE_MS) continue;
            try { client.close(4000, 'race over'); } catch (e) {}
            log(`SPECTATOR-CLOSE room=${roomId} user=${client.userId} acked=${acked} age_ms=${age}`);
          }
        }
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
  clearInterval(presenceTimer);   // R-49
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
