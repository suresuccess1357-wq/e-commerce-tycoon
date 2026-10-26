// Private Room relay. Stores nothing, logs nothing, cannot read anything.
// It only passes encrypted binary frames between the (max) two people in a room.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const page = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));

const HEADERS = {
  'Content-Type': 'text/html; charset=utf-8',
  'Cache-Control': 'no-store',
  'Referrer-Policy': 'no-referrer',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Content-Security-Policy':
    "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; " +
    "img-src blob:; media-src blob:; connect-src 'self' ws: wss:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
};

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') { res.writeHead(200, HEADERS); return res.end(page); }
  if (url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2 * 1024 * 1024 });
const rooms = new Map();   // roomId -> Set<ws>
const dead = new Map();    // destroyed roomId -> expiry timestamp (links stay dead for 24h)

const sendJSON = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const announce = set => { for (const s of set) sendJSON(s, { t: 'peers', n: set.size }); };

wss.on('connection', ws => {
  let room = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    if (!room) {                                   // first message must be a join
      if (isBinary) return ws.close();
      let m; try { m = JSON.parse(data.toString()); } catch { return ws.close(); }
      if (m.t !== 'join' || typeof m.room !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(m.room)) return ws.close();
      if (dead.has(m.room)) { sendJSON(ws, { t: 'destroyed' }); return ws.close(); }
      const set = rooms.get(m.room) || new Set();
      if (set.size >= 2) { sendJSON(ws, { t: 'full' }); return ws.close(); }
      set.add(ws); rooms.set(m.room, set); room = m.room;
      return announce(set);
    }
    const set = rooms.get(room);
    if (!set) return;
    if (!isBinary) {                               // only control message after joining: destroy
      let m; try { m = JSON.parse(data.toString()); } catch { return; }
      if (m.t === 'destroy') {
        dead.set(room, Date.now() + 24 * 3600 * 1000);
        for (const s of set) { sendJSON(s, { t: 'destroyed' }); s.close(); }
        rooms.delete(room);
      }
      return;
    }
    for (const s of set) if (s !== ws && s.readyState === 1) s.send(data, { binary: true });
  });

  ws.on('close', () => {
    if (!room) return;
    const set = rooms.get(room);
    if (!set) return;
    set.delete(ws);
    if (set.size === 0) rooms.delete(room); else announce(set);
  });
  ws.on('error', () => {});
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
  const now = Date.now();
  for (const [id, exp] of dead) if (exp < now) dead.delete(id);
}, 30000);

server.listen(PORT, () => console.log('Private Room relay listening on ' + PORT));
