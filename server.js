// Pixtown multiplayer server. Zero dependencies: plain Node (v16+).
// Realtime = Server-Sent Events (server -> browsers) + small POSTs (browsers -> server).
const http = require('http'), fs = require('fs'), path = require('path');
const PORT = process.env.PORT || 3000;
const N = 120;                                   // number of lots; keep in sync with public/index.html
const DATA = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const MAX_CLAIMS_PER_IP = Number(process.env.MAX_CLAIMS_PER_IP || 5);
const TIERS = ['House', 'Shop', 'Apartment', 'Tower'], ROOFS = ['flat', 'gable', 'none'], WINS = ['square', 'tall', 'round'];
const COLOR = /^#[0-9a-f]{6}$/i;

const CODES = {
  WELCOME: { reward: { money: 75 }, msg: '+$75' },
  SNACKTIME: { reward: { food: 60 }, msg: 'food +60' },
  MEDKIT: { reward: { hp: 50 }, msg: 'health +50' },
  GOLDRUSH: { reward: { money: 250 }, msg: '+$250' },
};
try { // extra codes via env: CODES='{"MYCODE":{"money":100,"food":0,"hp":0}}'
  const extra = JSON.parse(process.env.CODES || '{}');
  for (const k in extra) { const r = extra[k]; CODES[k.toUpperCase()] = { reward: { money: Math.min(1000, +r.money || 0), food: Math.min(100, +r.food || 0), hp: Math.min(100, +r.hp || 0) }, msg: 'reward received' }; }
} catch (e) {}
const RFILE = DATA.replace(/\.json$/, '') + '-redeemed.json';
let redeemed = {}; try { redeemed = JSON.parse(fs.readFileSync(RFILE, 'utf8')); } catch (e) {}
let rdirty = false; const attempts = new Map();
setInterval(() => { if (!rdirty) return; rdirty = false; fs.writeFile(RFILE, JSON.stringify(redeemed), () => {}); }, 2000);
let lots = {};
try { lots = JSON.parse(fs.readFileSync(DATA, 'utf8')); } catch (e) {}
let dirty = false;
setInterval(() => { if (!dirty) return; dirty = false; fs.writeFile(DATA, JSON.stringify(lots), () => {}); }, 2000);
process.on('SIGINT', () => { try { fs.writeFileSync(DATA, JSON.stringify(lots)); fs.writeFileSync(RFILE, JSON.stringify(redeemed)); } catch (e) {} process.exit(); });
process.on('SIGTERM', () => { try { fs.writeFileSync(DATA, JSON.stringify(lots)); fs.writeFileSync(RFILE, JSON.stringify(redeemed)); } catch (e) {} process.exit(); });

const clients = new Set();                       // {res, pid}
const pos = new Map();                           // pid -> {x, t}
const ipClaims = new Map();
const send = (res, ev, d) => res.write('event: ' + ev + '\ndata: ' + JSON.stringify(d) + '\n\n');
const bcast = (ev, d) => clients.forEach(c => send(c.res, ev, d));

let lastPeers = '';
setInterval(() => {
  const now = Date.now();
  for (const [p, v] of pos) if (now - v.t > 5000) pos.delete(p);
  const list = [...pos].map(([pid, v]) => ({ pid, x: v.x, z: v.z, r: v.r }));
  const s = JSON.stringify(list);
  if (s !== lastPeers) { lastPeers = s; bcast('peers', list); }
}, 500);
setInterval(() => clients.forEach(c => c.res.write(': hb\n\n')), 20000);

const ok = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
const goodPid = p => typeof p === 'string' && p.length >= 8 && p.length <= 80;
const cleanName = s => String(s || '').replace(/[^\w &'.\-]/g, '').trim().slice(0, 13);
function cleanLook(b, base) {
  const o = Object.assign({}, base);
  if (COLOR.test(b.wall)) o.wall = b.wall;
  if (COLOR.test(b.door)) o.door = b.door;
  if (ROOFS.includes(b.roof)) o.roof = b.roof;
  if (WINS.includes(b.win)) o.win = b.win;
  return o;
}
function body(req) {
  return new Promise(r => {
    let s = '';
    req.on('data', c => { s += c; if (s.length > 4096) req.destroy(); });
    req.on('end', () => { try { r(JSON.parse(s)); } catch (e) { r({}); } });
    req.on('error', () => r({}));
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    const f = fs.existsSync(path.join(__dirname, 'index.html')) ? path.join(__dirname, 'index.html') : path.join(__dirname, 'public', 'index.html');
    return fs.readFile(f, (e, buf) => {
      if (e) { res.writeHead(500); return res.end('missing index.html'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); res.end(buf);
    });
  }
  if (req.method === 'GET' && url.pathname === '/api/events') {
    const pid = url.searchParams.get('pid') || '';
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.write('retry: 2000\n\n');
    const c = { res, pid }; clients.add(c);
    send(res, 'state', lots); send(res, 'peers', JSON.parse(lastPeers || '[]'));
    req.on('close', () => { clients.delete(c); if (goodPid(pid) && ![...clients].some(x => x.pid === pid)) pos.delete(pid); });
    return;
  }
  if (req.method !== 'POST' || !url.pathname.startsWith('/api/')) { res.writeHead(404); return res.end('not found'); }
  const b = await body(req);
  if (!goodPid(b.pid)) return ok(res, 400, { error: 'bad player id' });

  if (url.pathname === '/api/pos') {
    if ([b.x, b.z, b.r].every(v => typeof v === 'number' && isFinite(v))) pos.set(b.pid, { x: Math.max(-100, Math.min(1e5, b.x)), z: Math.max(-100, Math.min(100, b.z)), r: b.r, t: Date.now() });
    return ok(res, 200, {});
  }
  if (url.pathname === '/api/redeem') {
    const code = String(b.code || '').trim().toUpperCase().slice(0, 40), now = Date.now();
    const a = (attempts.get(b.pid) || []).filter(x => now - x < 60000);
    if (a.length >= 10) return ok(res, 429, { error: 'Too many tries. Wait a minute.' });
    a.push(now); attempts.set(b.pid, a);
    const def = CODES[code];
    if (!def) return ok(res, 404, { error: 'Invalid code' });
    if (redeemed[b.pid + ':' + code]) return ok(res, 409, { error: 'Code already used' });
    redeemed[b.pid + ':' + code] = 1; rdirty = true;
    return ok(res, 200, { ok: true, msg: def.msg, reward: def.reward });
  }
  const i = b.i;
  if (!Number.isInteger(i) || i < 0 || i >= N) return ok(res, 400, { error: 'bad lot' });
  const key = 'L' + i;

  if (url.pathname === '/api/claim') {
    if (lots[key]) return ok(res, 409, { error: 'Someone just took that lot' });
    if (Object.values(lots).some(l => l.pid === b.pid)) return ok(res, 409, { error: 'You already own a house' });
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if ((ipClaims.get(ip) || 0) >= MAX_CLAIMS_PER_IP) return ok(res, 429, { error: 'Too many claims from this network' });
    ipClaims.set(ip, (ipClaims.get(ip) || 0) + 1);
    lots[key] = { pid: b.pid, name: '', tier: 'House', wall: '#e07a5f', roof: 'gable', win: 'square', door: '#6b3f2a' };
    dirty = true; bcast('lot', { i, d: lots[key] });
    return ok(res, 200, { ok: true });
  }
  if (url.pathname === '/api/save') {
    const cur = lots[key];
    if (!cur || cur.pid !== b.pid) return ok(res, 403, { error: 'Not your house' });
    const next = cleanLook(b, cur);
    if (!cur.name) {                              // name and size lock after the first save
      next.name = cleanName(b.name) || 'My Place';
      if (TIERS.includes(b.tier)) next.tier = b.tier;
    }
    lots[key] = next; dirty = true; bcast('lot', { i, d: next });
    return ok(res, 200, { ok: true });
  }
  ok(res, 404, { error: 'unknown endpoint' });
});
server.listen(PORT, () => console.log('Pixtown running on http://localhost:' + PORT));
