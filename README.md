# OneTraxx WebSocket Server

A minimal Node.js WebSocket relay for OneTraxx real-time race traffic. It replaces
Supabase Realtime for the high-volume / latency-sensitive events (GPS, chat, voice,
cheers, finish/quit/penalty signals) and adds **server-side GPS batching**: each
racer sends their own position individually, and the server fans out **one** combined
batch per room per second.

Supabase Realtime stays in the app as the last-resort fallback and continues to own
the things that must remain DB-driven (`room_members` INSERT/DELETE, `race_rooms`
UPDATE, presence). This server does **not** touch those.

```
Primary:     wss://ws.onetraxx.com         (Oracle Cloud VM — not ready yet)
Backup:      wss://ws-backup.onetraxx.com  (Render — deploy here first)
Last resort: Supabase Realtime             (existing in-app code, kept intact)
```

## What it handles

| Direction | Event(s) | Behaviour |
|-----------|----------|-----------|
| client → server | `gps` | Buffered per racer; fanned out as one `gps_batch` every 1000 ms |
| client → server | `request_positions` | Replies to **that client only** with the current buffer as `gps_batch` |
| client → server | `chat`, `voice_msg`, `cheer`, `finished`, `racer_quit`, `false_start`, `race_over` | Relayed immediately to **everyone else** in the room |
| server → clients | `gps_batch` | `{ racers: [{ user_id, distance_m, speed_kmh, ts }], ts }` to **all** members (incl. sender) |
| server → clients | `racer_quit` | Emitted automatically when a racer disconnects |

`send_positions` (the old peer-to-peer catch-up) is **obsolete** here — the server
owns the position buffer, so `request_positions` is answered directly by the server.

### Message envelope

All frames are JSON: `{ "event": "<name>", "payload": { ... } }`.

### Connection URL

```
wss://<host>/?roomId=<roomId>&userId=<userId>&token=<supabase_access_token>&role=racer|spectator
```

On connect the server calls `supabase.auth.getUser(token)` once. If the token is
invalid/expired, or its user id doesn't match `userId`, the socket is closed.

### Close codes

| Code | Meaning |
|------|---------|
| 4000 | Missing/invalid query params, or connection replaced by a newer one for the same user |
| 4001 | Auth failed (bad/expired token, or token/user mismatch) |
| 4002 | Room at capacity (8 racers / 20 spectators) |

### Limits

- Max **8 racers** and **20 spectators** per room. The app enforces these; the
  server rejects over-cap connections as a backstop.

### Health check

`GET /` → `{ "status": "ok", "rooms": <N>, "connections": <N> }`

---

## Local development

```bash
cp .env.example .env        # then paste the real SUPABASE_SERVICE_ROLE_KEY
npm install
npm start                   # listens on PORT (default 8080)
curl http://localhost:8080/ # -> {"status":"ok","rooms":0,"connections":0}
```

Get the service-role key from **Supabase Dashboard → Project Settings → API →
`service_role` secret**. It bypasses RLS — never ship it to the app or commit it.

---

## Deploy: Render (backup — do this first)

Hosts `wss://ws-backup.onetraxx.com`. Render terminates TLS and proxies WebSockets
to the service over the same HTTPS port, so no extra WS config is needed.

### Option A — Blueprint (render.yaml)

1. Push this directory to its own Git repo (GitHub/GitLab).
2. Render → **New → Blueprint** → pick the repo. It reads `render.yaml`.
3. When prompted, set **`SUPABASE_SERVICE_ROLE_KEY`** (marked `sync: false`).
4. Deploy. Confirm the health check at `https://<service>.onrender.com/`.

### Option B — Manual

1. Render → **New → Web Service** → connect the repo.
2. Runtime **Node**, Build `npm install`, Start `node server.js`, Health check `/`.
3. Env vars: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `PORT=8080`.

> Render provides its own `PORT`; the server honours `process.env.PORT`, so it works
> whether or not you pin `PORT`.

### Custom domain

Render service → **Settings → Custom Domains** → add `ws-backup.onetraxx.com`, then
add the shown `CNAME` at your DNS provider. Connect from the app with
`wss://ws-backup.onetraxx.com/?...`.

> The free plan idles after inactivity and cold-starts (~tens of seconds). Fine for a
> backup; use a paid plan or the Oracle VM for the primary.

---

## Deploy: Oracle Cloud VM (primary — when ready)

Hosts `wss://ws.onetraxx.com`. Pattern: Node app on `:8080` behind nginx doing TLS
and the WebSocket upgrade proxy, kept alive by `pm2` (or systemd).

### 1. Provision & install

```bash
# Ubuntu 22.04 ARM (Ampere A1) or x86
sudo apt update && sudo apt install -y nginx
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm install -g pm2
```

Open **80** and **443** in the Oracle **security list / NSG** and the host firewall:

```bash
sudo iptables -I INPUT -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 443 -j ACCEPT
sudo netfilter-persistent save   # if installed
```

### 2. App

```bash
git clone <your-repo> /opt/onetraxx-ws && cd /opt/onetraxx-ws
npm install --omit=dev
cp .env.example .env   # set SUPABASE_SERVICE_ROLE_KEY; keep PORT=8080
pm2 start server.js --name onetraxx-ws
pm2 save && pm2 startup   # run the printed command to survive reboots
```

### 3. nginx reverse proxy with WebSocket upgrade

`/etc/nginx/sites-available/onetraxx-ws`:

```nginx
server {
    listen 80;
    server_name ws.onetraxx.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;       # WebSocket upgrade
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 3600s;                     # keep long-lived sockets open
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/onetraxx-ws /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

### 4. TLS (Let's Encrypt)

Point `ws.onetraxx.com` (A record) at the VM's public IP first, then:

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d ws.onetraxx.com   # auto-rewrites the server block to 443
```

Verify: `curl https://ws.onetraxx.com/` → `{"status":"ok",...}`, then connect with
`wss://ws.onetraxx.com/?...`.

### Updating

```bash
cd /opt/onetraxx-ws && git pull && npm install --omit=dev && pm2 reload onetraxx-ws
```

`pm2 logs onetraxx-ws` for the JOIN/LEAVE/REJECT logs.

---

## Notes

- Plain JavaScript, no TypeScript, no Express (built-in `http` for the health check).
- Dependencies: `ws`, `@supabase/supabase-js`, `dotenv` only.
- 30 s ping/pong sweep terminates dead sockets; a reconnect for the same `userId`
  transparently replaces the stale connection (close 4000) without a spurious quit.
