#!/usr/bin/env python3
"""
GREEN LEDGER - Decentralized / Distributed Ledger (DLT) data store
Only standard library is used. Local run:  python main.py   ->  http://127.0.0.1:8000
Render: Start Command = python main.py  (uses $PORT, binds 0.0.0.0)

Features: Add data, Download data, Remove data (tombstone block), 3 nodes, consensus sync.
Dashboards: Overview, Data Vault, Coins, NFTs, Ledger Explorer, Network Nodes, Blockchain Visual (glowing node network).
"""
import json, hashlib, time, base64, os, threading, webbrowser
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, quote

DB = os.path.join(os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__))), "ledger_data.json")
NODES = ["Node-A", "Node-B", "Node-C"]
DIFF = 3                      # proof-of-work: hash must start with "000"
MAX_B64 = 3 * 1024 * 1024     # ~2.2MB file limit
LOCK = threading.Lock()
nodes = {}

sha = lambda s: hashlib.sha256(s.encode()).hexdigest()
CORE = ("index", "time", "action", "category", "title", "filename", "mime", "size", "content_hash", "ref", "prev", "nonce")


def block_hash(b):
    return sha(json.dumps({k: b[k] for k in CORE}, sort_keys=True))


def make(chain, **kw):
    b = dict(index=len(chain), time=time.time(), prev=chain[-1]["hash"] if chain else "0" * 64,
             action="ADD", category="data", title="", filename="", mime="text/plain", ref=None, content="")
    b.update(kw)
    b["content_hash"] = sha(b["content"])
    b["size"] = len(b["content"]) * 3 // 4
    b["nonce"] = 0
    while not block_hash(b).startswith("0" * DIFF):
        b["nonce"] += 1
    b["hash"] = block_hash(b)
    return b


def valid(chain):
    for i, b in enumerate(chain):
        try:
            if b["index"] != i or b["hash"] != block_hash(b) or not b["hash"].startswith("0" * DIFF): return False
            if b["content_hash"] != sha(b["content"]): return False
            if b["prev"] != ("0" * 64 if i == 0 else chain[i - 1]["hash"]): return False
        except KeyError:
            return False
    return bool(chain)


def save():
    with open(DB, "w") as f: json.dump(nodes, f)


def load():
    global nodes
    try:
        with open(DB) as f: nodes = json.load(f)
    except Exception:
        g = make([], action="GENESIS", category="system", title="Genesis Block")
        nodes = {n: [json.loads(json.dumps(g))] for n in NODES}
        save()


def best():
    ok = [c for c in nodes.values() if valid(c)]
    return max(ok, key=len) if ok else nodes[NODES[0]]


def state():
    return {"chain": [{k: v for k, v in b.items() if k != "content"} for b in best()],
            "nodes": [{"name": n, "length": len(c), "valid": valid(c), "head": c[-1]["hash"] if c else ""} for n, c in nodes.items()],
            "diff": DIFF}


def commit(blk, prev_hash):
    for ch in nodes.values():          # broadcast to every node that is in sync
        if ch and ch[-1]["hash"] == prev_hash: ch.append(json.loads(json.dumps(blk)))
    save()


def add(d):
    title, content = str(d.get("title", "")).strip()[:120], str(d.get("content", ""))
    cat = d.get("category", "data")
    if not title or not content: return {"ok": False, "msg": "Title and data are required"}
    if len(content) > MAX_B64 or cat not in ("data", "coin", "nft"): return {"ok": False, "msg": "Data too large / invalid category"}
    chain = best()
    blk = make(chain, category=cat, title=title, filename=str(d.get("filename", ""))[:120] or title[:40] + ".txt",
               mime=str(d.get("mime", "text/plain"))[:80] or "application/octet-stream", content=content)
    commit(blk, chain[-1]["hash"])
    return {"ok": True, "msg": f"Block #{blk['index']} mined (nonce {blk['nonce']})"}


def remove(d):
    chain = best()
    try: i = int(d["id"])
    except Exception: return {"ok": False, "msg": "Invalid id"}
    gone = {b["ref"] for b in chain if b["action"] == "REMOVE"}
    if i <= 0 or i >= len(chain) or chain[i]["action"] != "ADD" or i in gone:
        return {"ok": False, "msg": "Record not found / already removed"}
    blk = make(chain, action="REMOVE", category=chain[i]["category"], title=f"Removed record #{i}", ref=i)
    commit(blk, chain[-1]["hash"])
    return {"ok": True, "msg": f"Record #{i} removed (tombstone block #{blk['index']})"}


def tamper(d):
    ch = nodes.get(d.get("node"))
    if not ch: return {"ok": False, "msg": "Node not found"}
    ch[-1]["title"] += " (tampered)"
    save()
    return {"ok": True, "msg": f"{d['node']} data altered - validation will now fail"}


def sync(_):
    b = best()
    for n in NODES: nodes[n] = json.loads(json.dumps(b))
    save()
    return {"ok": True, "msg": "Consensus reached: all nodes synced to the longest valid chain"}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def reply(self, code, body, ctype="application/json", extra=None):
        if not isinstance(body, bytes): body = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items(): self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path == "/": return self.reply(200, HTML.encode(), "text/html; charset=utf-8")
        with LOCK:
            if u.path == "/api/chain": return self.reply(200, state())
            if u.path == "/api/download":
                try: b = best()[int(q["id"][0])]
                except Exception: return self.reply(404, {"error": "not found"})
                fn = b["filename"] or f"block-{b['index']}.txt"
                return self.reply(200, base64.b64decode(b["content"]), b["mime"] or "application/octet-stream",
                                  {"Content-Disposition": "inline; filename*=UTF-8''" + quote(fn)})
        self.reply(404, {"error": "not found"})

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()

    def do_POST(self):
        try: d = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        except Exception: return self.reply(400, {"ok": False, "msg": "bad json"})
        fn = {"/api/add": add, "/api/remove": remove, "/api/tamper": tamper, "/api/sync": sync}.get(urlparse(self.path).path)
        if not fn: return self.reply(404, {"ok": False, "msg": "not found"})
        with LOCK: self.reply(200, fn(d))


HTML = r'''<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GREEN LEDGER · Distributed Data Store</title>
<style>
:root{
  --bg-card:rgba(34,40,59,.55);--accent-pink:#f95c8b;--accent-purple:#b45cf9;--accent-blue:#3b8bff;--accent-green:#2edb7b;
  --text-light:#eef2ff;--text-muted:#8b95b5;--shadow-card:0 20px 40px -12px rgba(0,0,0,.6);--radius:22px;
  --transition:all .3s cubic-bezier(.2,.9,.4,1);
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;scroll-behavior:smooth}
body{font-family:'Inter','Segoe UI',system-ui,-apple-system,sans-serif;
background:linear-gradient(145deg,#0e121c 0%,#1a1f2e 100%);color:var(--text-light);
display:flex;line-height:1.5;overflow-x:hidden}
body::before{content:"";position:fixed;inset:0;
background:url("https://static.vecteezy.com/system/resources/thumbnails/034/860/779/small_2x/abstract-digital-connection-dots-and-lines-background-for-technology-artificial-intelligence-science-global-communication-ai-generative-photo.jpg") center/cover fixed;
opacity:.1;pointer-events:none;z-index:0;animation:subtleDrift 40s infinite alternate ease-in-out}
@keyframes subtleDrift{0%{transform:scale(1) rotate(0deg);opacity:.08}100%{transform:scale(1.1) rotate(1deg);opacity:.15}}
aside{width:260px;padding:28px 16px;background:rgba(18,22,35,.35);
backdrop-filter:blur(22px);-webkit-backdrop-filter:blur(22px);
border-right:1px solid rgba(255,255,255,.06);display:flex;flex-direction:column;gap:8px;
position:sticky;top:0;height:100vh;z-index:10;box-shadow:8px 0 30px rgba(0,0,0,.3)}
.logo{font-weight:800;letter-spacing:2px;font-size:20px;
background:linear-gradient(135deg,#f95c8b,#b45cf9,#3b8bff);
-webkit-background-clip:text;background-clip:text;color:transparent;margin-bottom:24px;padding-left:6px}
.logo small{display:block;font-size:10px;font-weight:400;letter-spacing:2px;
color:var(--text-muted);margin-top:4px;-webkit-text-fill-color:var(--text-muted)}
nav{display:flex;flex-direction:column;gap:4px;flex:1}
nav button{all:unset;cursor:pointer;padding:12px 16px;border-radius:14px;
color:var(--text-muted);font-weight:500;font-size:15px;transition:var(--transition);
display:flex;align-items:center;gap:12px;border:1px solid transparent}
nav button::before{content:"●";font-size:8px;opacity:.4;transition:var(--transition);color:var(--accent-pink)}
nav button:hover{color:var(--text-light);background:rgba(255,255,255,.05);border-color:rgba(255,255,255,.08);transform:translateX(4px)}
nav button.on{color:#fff;background:linear-gradient(90deg,rgba(249,92,139,.2),rgba(180,92,249,.1));
border-color:rgba(249,92,139,.3);box-shadow:0 0 20px -6px rgba(249,92,139,.4)}
nav button.on::before{opacity:1;text-shadow:0 0 8px var(--accent-pink)}
main{flex:1;padding:32px 40px;min-width:0;position:relative;z-index:1}
h1{font-weight:700;font-size:28px;letter-spacing:-.5px;background:linear-gradient(135deg,#fff,#c7d2ff);
-webkit-background-clip:text;background-clip:text;color:transparent;margin-bottom:4px}
.sub{color:var(--text-muted);font-size:14px;margin-bottom:24px;letter-spacing:.2px}
.grid{display:grid;gap:18px;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));margin-bottom:24px}
.card{background:var(--bg-card);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
border-radius:var(--radius);padding:20px 22px;border:1px solid rgba(255,255,255,.08);
box-shadow:var(--shadow-card);transition:var(--transition);position:relative;overflow:hidden}
.card:hover{transform:translateY(-3px);border-color:rgba(249,92,139,.3);box-shadow:0 24px 48px -16px rgba(249,92,139,.2)}
.grid .card{margin:0}
.stat b{display:block;font-size:34px;font-weight:700;background:linear-gradient(135deg,#fff,#b9c6ff);
-webkit-background-clip:text;background-clip:text;color:transparent}
.stat span{color:var(--text-muted);font-size:12px;text-transform:uppercase;letter-spacing:1.2px;font-weight:500}
input,textarea,select{width:100%;padding:12px 16px;margin:6px 0 14px;background:rgba(10,14,22,.5);
border:1px solid rgba(255,255,255,.08);border-radius:14px;color:var(--text-light);font:inherit;font-size:14px;outline:none;transition:var(--transition)}
input:focus,textarea:focus,select:focus{border-color:var(--accent-pink);box-shadow:0 0 0 3px rgba(249,92,139,.15)}
label{font-size:12px;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;font-weight:600;display:block;margin-bottom:2px}
.btn{cursor:pointer;border:1px solid rgba(255,255,255,.1);background:linear-gradient(135deg,rgba(249,92,139,.25),rgba(180,92,249,.2));
color:#fff;padding:10px 20px;border-radius:14px;font-weight:600;font-size:14px;transition:var(--transition);font-family:inherit;
display:inline-flex;align-items:center;justify-content:center;gap:6px}
.btn:hover{background:linear-gradient(135deg,rgba(249,92,139,.4),rgba(180,92,249,.35));border-color:rgba(249,92,139,.5);transform:translateY(-2px)}
.btn.red{background:rgba(255,77,109,.15);border-color:rgba(255,77,109,.3);color:#ff7a93}
.btn.red:hover{background:rgba(255,77,109,.3);border-color:var(--accent-pink)}
.btn.sm{padding:6px 14px;font-size:12px;border-radius:10px}
table{width:100%;border-collapse:collapse;font-size:14px}
th{color:var(--text-muted);text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:1.2px;padding:10px 8px;font-weight:600}
td{padding:12px 8px;border-top:1px solid rgba(255,255,255,.06);vertical-align:middle;color:#d0d9f0}
tr:hover td{background:rgba(255,255,255,.03)}
.tbl{overflow-x:auto;border-radius:12px}
.hash{font-family:'JetBrains Mono','Consolas',monospace;color:#8fa9ff;font-size:12px;word-break:break-all}
.tag{display:inline-block;padding:3px 12px;border-radius:30px;font-size:11px;font-weight:600;background:rgba(249,92,139,.15);color:#f95c8b;border:1px solid rgba(249,92,139,.25)}
.tag.bad{background:rgba(255,77,109,.2);color:#ff4d6d;border-color:rgba(255,77,109,.35)}
.two{display:grid;gap:22px;grid-template-columns:minmax(260px,1fr) 2fr}
.nft{display:grid;gap:18px;grid-template-columns:repeat(auto-fill,minmax(190px,1fr))}
.nft .card{margin:0;padding:14px;border-radius:18px}
.nft img{width:100%;height:150px;object-fit:cover;border-radius:14px;background:#0e121c;border:1px solid rgba(255,255,255,.08)}
.blk{border-left:3px solid #b45cf9;margin-bottom:16px;transition:var(--transition);background:rgba(255,255,255,.02);border-radius:0 12px 12px 0;padding:12px 0 12px 18px}
.blk:hover{background:rgba(255,255,255,.05);transform:translateX(4px)}
#toast{position:fixed;right:28px;bottom:28px;background:rgba(30,36,56,.9);border:1px solid rgba(249,92,139,.5);color:#fff;padding:14px 24px;
border-radius:16px;opacity:0;transition:all .4s;z-index:2000;font-size:14px;transform:translateY(20px);pointer-events:none}
#toast.show{opacity:1;transform:translateY(0)}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-thumb{background:rgba(249,92,139,.4);border-radius:8px}
@keyframes fadeSlide{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
main>*{animation:fadeSlide .4s ease-out}

/* ===== NEW: GLOWING NODE-CHAIN VISUAL ===== */
.vwrap{position:relative;width:100%;border-radius:24px;overflow-x:auto;overflow-y:hidden;
background:radial-gradient(circle at 15% 25%,rgba(59,139,255,.14),transparent 45%),radial-gradient(circle at 70% 70%,rgba(46,219,123,.14),transparent 50%),#050a0c;
border:1px solid rgba(46,219,123,.15);box-shadow:inset 0 0 80px rgba(0,0,0,.7),0 20px 60px -20px rgba(0,0,0,.8)}
.vwrap canvas{display:block}
.vleg{display:flex;gap:18px;flex-wrap:wrap;margin-top:14px;font-size:12px;color:var(--text-muted)}
.vleg i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:6px;vertical-align:middle}
#vtip{position:fixed;pointer-events:none;z-index:900;padding:8px 14px;border-radius:12px;font-size:12px;
background:rgba(10,16,20,.92);border:1px solid rgba(46,219,123,.5);box-shadow:0 0 24px -4px rgba(46,219,123,.6);
opacity:0;transform:translateY(6px);transition:opacity .15s,transform .15s;max-width:260px}
#vtip.show{opacity:1;transform:translateY(0)}

/* modal */
.modal-bg{position:fixed;inset:0;background:rgba(4,7,12,.78);backdrop-filter:blur(10px);display:none;align-items:center;justify-content:center;z-index:1000;padding:22px}
.modal-bg.show{display:flex;animation:fadeSlide .25s ease-out}
.modal{width:100%;max-width:580px;max-height:88vh;overflow:auto;background:linear-gradient(160deg,rgba(28,34,52,.98),rgba(16,21,32,.98));
border:1px solid rgba(46,219,123,.4);border-radius:22px;padding:24px 26px;box-shadow:0 30px 80px -20px rgba(0,0,0,.9),0 0 50px -10px rgba(46,219,123,.4)}
.modal h2{font-size:20px;font-weight:700;margin-bottom:4px;background:linear-gradient(135deg,#fff,#c7d2ff);-webkit-background-clip:text;background-clip:text;color:transparent}
.modal .m-sub{color:var(--text-muted);font-size:12px;margin-bottom:16px}
.modal .row{display:flex;justify-content:space-between;gap:12px;padding:9px 0;border-top:1px solid rgba(255,255,255,.06);font-size:13.5px}
.modal .row:first-of-type{border-top:none}
.modal .k{color:var(--text-muted);font-weight:600;font-size:12px;text-transform:uppercase}
.modal .v{color:#e6ecff;text-align:right;word-break:break-all;font-family:'JetBrains Mono','Consolas',monospace;font-size:12px}
.pv{margin-top:14px;background:rgba(0,0,0,.4);border:1px solid rgba(46,219,123,.25);border-radius:12px;padding:12px;max-height:230px;overflow:auto;
font-size:12px;white-space:pre-wrap;word-break:break-all;font-family:'JetBrains Mono','Consolas',monospace;color:#cfe;}
.pv img{max-width:100%;border-radius:8px;display:block;margin-bottom:8px}
.m-btns{display:flex;gap:10px;margin-top:16px}.m-btns .btn{flex:1}

@media(max-width:800px){
  body{flex-direction:column}
  aside{width:100%;height:auto;position:static;flex-direction:row;flex-wrap:wrap;padding:16px 18px;border-right:none}
  .logo{width:100%;margin-bottom:8px;font-size:17px}
  nav{flex-direction:row;flex-wrap:wrap}
  nav button{padding:8px 14px;font-size:13px}
  main{padding:24px 18px}.two{grid-template-columns:1fr}.stat b{font-size:24px}
}
@media(max-width:480px){.btn{font-size:13px;padding:8px 16px}.btn.sm{width:auto}}
</style></head>
<body>
<aside>
  <div class="logo">GREEN LEDGER<small>DISTRIBUTED DATA STORE</small></div>
  <nav id="nav"></nav>
</aside>
<main id="main">Loading...</main>
<div id="toast"></div>
<div id="vtip"></div>

<div class="modal-bg" id="blockModal" onclick="if(event.target===this)closeBlock()">
  <div class="modal" id="blockModalBody"></div>
</div>

<script>
const $=s=>document.querySelector(s);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt=n=>n<1024?n+' B':n<1048576?(n/1024).toFixed(1)+' KB':(n/1048576).toFixed(2)+' MB';
const dt=t=>new Date(t*1000).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
const b64=s=>btoa(unescape(encodeURIComponent(s)));
const VIEWS={overview:'Overview',vault:'Data Vault',coins:'Coins',nfts:'NFTs',explorer:'Ledger Explorer',visual:'Blockchain Visual',nodes:'Network Nodes'};
let S=null,view='overview';
function toast(m){const t=$('#toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),3200)}
async function api(p,b){const r=await fetch('/api/'+p,b?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)}:{});return r.json()}
async function act(p,b){const r=await api(p,b);toast(r.msg);await load()}
async function load(){S=await api('chain');draw()}
function recs(cat){const rm=new Set(S.chain.filter(b=>b.action=='REMOVE').map(b=>b.ref));return S.chain.filter(b=>b.action=='ADD'&&!rm.has(b.index)&&(!cat||b.category==cat))}
function readFile(f,max){return new Promise((ok,no)=>{if(f.size>max)return no('File too large (max '+fmt(max)+')');const r=new FileReader();r.onload=()=>ok(r.result);r.onerror=no;r.readAsDataURL(f)})}
const dl=id=>location.href='/api/download?id='+id;
const rm=id=>confirm('Remove record #'+id+'? (a tombstone block will be added)')&&act('remove',{id});
function rows(list){
  if(!list.length)return '<p class="sub">No records yet.</p>';
  return `<div class="tbl"><table><tr><th>#</th><th>Title</th><th>Size</th><th>Time</th><th>Hash</th><th></th></tr>${list.map(b=>`<tr><td>${b.index}</td><td>${esc(b.title)}</td><td>${fmt(b.size)}</td><td>${dt(b.time)}</td><td class="hash">${b.hash.slice(0,14)}...</td><td style="white-space:nowrap"><button class="btn sm" onclick="dl(${b.index})">Download</button> <button class="btn sm red" onclick="rm(${b.index})">Remove</button></td></tr>`).join('')}</table></div>`}

/* ================= GLOWING NODE-CHAIN VISUAL (canvas) ================= */
let VZ=null;
function stopVisual(){if(VZ){cancelAnimationFrame(VZ.raf);removeEventListener('resize',VZ.rs);VZ=null}$('#vtip').classList.remove('show')}
function visual(){
  return `<h1>Blockchain Visual</h1><div class="sub">Glowing node network · hover karo effect ke liye, block par click karo to full data dekho</div>
  <div class="vwrap" id="vwrap"><canvas id="vc"></canvas></div>
  <div class="vleg"><span><i style="background:#3b8bff"></i>Genesis</span><span><i style="background:#2edb7b"></i>Data / NFT</span><span><i style="background:#b45cf9"></i>Coin</span><span><i style="background:#ff4d6d"></i>Remove (tombstone)</span></div>`}
function initVisual(){
  const wrap=$('#vwrap'),cv=$('#vc'),ctx=cv.getContext('2d'),tip=$('#vtip');
  const chain=S.chain,N=chain.length,GAP=175,dpr=window.devicePixelRatio||1;
  let W,H,nd=[],parts=[],bokeh=[],hover=-1,mx=-999,my=-999;
  const col=b=>b.action=='GENESIS'?[59,139,255]:b.action=='REMOVE'?[255,77,109]:b.category=='coin'?[180,92,249]:[46,219,123];
  const rgba=(c,a)=>`rgba(${c[0]},${c[1]},${c[2]},${a})`;
  function build(){
    W=Math.max(wrap.clientWidth,N*GAP+140);H=Math.max(440,Math.min(560,innerHeight-230));
    cv.width=W*dpr;cv.height=H*dpr;cv.style.width=W+'px';cv.style.height=H+'px';ctx.setTransform(dpr,0,0,dpr,0,0);
    nd=chain.map((b,i)=>({b,c:col(b),x:90+i*GAP,y:H/2+(i%2?1:-1)*(45+((i*37)%45)),rot:(((i*53)%13)-6)*Math.PI/180,sc:1,ph:i*1.3}));
    parts=Array.from({length:Math.min(110,Math.round(W/16))},()=>({x:Math.random()*W,y:Math.random()*H,vx:(Math.random()-.5)*.3,vy:(Math.random()-.5)*.3,r:Math.random()*1.6+.6}));
    bokeh=Array.from({length:Math.round(W/260)+4},()=>({x:Math.random()*W,y:Math.random()*H,r:30+Math.random()*60,vx:(Math.random()-.5)*.12,vy:(Math.random()-.5)*.12}));
  }
  function rr(x,y,w,h,r){ctx.beginPath();ctx.roundRect?ctx.roundRect(x,y,w,h,r):ctx.rect(x,y,w,h)}
  function frame(t){
    ctx.clearRect(0,0,W,H);
    // bokeh blobs
    bokeh.forEach(o=>{o.x+=o.vx;o.y+=o.vy;if(o.x<-100)o.x=W+100;if(o.x>W+100)o.x=-100;if(o.y<-100)o.y=H+100;if(o.y>H+100)o.y=-100;
      const g=ctx.createRadialGradient(o.x,o.y,0,o.x,o.y,o.r);g.addColorStop(0,'rgba(120,160,150,.10)');g.addColorStop(1,'rgba(120,160,150,0)');ctx.fillStyle=g;ctx.beginPath();ctx.arc(o.x,o.y,o.r,0,7);ctx.fill()});
    // background mesh
    parts.forEach(p=>{p.x+=p.vx;p.y+=p.vy;if(p.x<0||p.x>W)p.vx*=-1;if(p.y<0||p.y>H)p.vy*=-1});
    for(let i=0;i<parts.length;i++){const a=parts[i];
      for(let j=i+1;j<parts.length;j++){const b=parts[j],d=Math.hypot(a.x-b.x,a.y-b.y);
        if(d<125){ctx.strokeStyle=`rgba(80,200,160,${(1-d/125)*.16})`;ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke()}}
      ctx.fillStyle='rgba(120,230,190,.35)';ctx.beginPath();ctx.arc(a.x,a.y,a.r,0,7);ctx.fill()}
    // chain links + travelling pulses
    for(let i=0;i<N-1;i++){const a=nd[i],b=nd[i+1];
      const g=ctx.createLinearGradient(a.x,a.y,b.x,b.y);g.addColorStop(0,rgba(a.c,.75));g.addColorStop(1,rgba(b.c,.75));
      ctx.save();ctx.strokeStyle=g;ctx.lineWidth=1.6;ctx.shadowColor=rgba(b.c,.9);ctx.shadowBlur=8;
      ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke();
      // static mid dot (like reference)
      ctx.fillStyle='#2edb7b';ctx.shadowBlur=12;ctx.beginPath();ctx.arc((a.x+b.x)/2,(a.y+b.y)/2,3.4,0,7);ctx.fill();
      // moving pulse
      const k=((t*.00035)+i*.27)%1,px=a.x+(b.x-a.x)*k,py=a.y+(b.y-a.y)*k;
      ctx.fillStyle='#fff';ctx.shadowColor='#7dffc0';ctx.shadowBlur=16;ctx.beginPath();ctx.arc(px,py,2.6,0,7);ctx.fill();ctx.restore()}
    // blocks
    nd.forEach((n,i)=>{
      const target=hover==i?1.22:1;n.sc+=(target-n.sc)*.15;
      const pulse=.5+.5*Math.sin(t*.002+n.ph),R=78*(hover==i?1.35:1)+pulse*8;
      const g=ctx.createRadialGradient(n.x,n.y,0,n.x,n.y,R);g.addColorStop(0,rgba(n.c,.42+pulse*.1));g.addColorStop(.55,rgba(n.c,.14));g.addColorStop(1,rgba(n.c,0));
      ctx.fillStyle=g;ctx.beginPath();ctx.arc(n.x,n.y,R,0,7);ctx.fill();
      // orbiting sparks
      for(let k=0;k<7;k++){const ang=t*.0006*(k%2?1:-1)+k*.9+n.ph,rad=40+(k%3)*4;
        ctx.fillStyle=rgba(n.c,.55);ctx.beginPath();ctx.arc(n.x+Math.cos(ang)*rad,n.y+Math.sin(ang)*rad,1.4,0,7);ctx.fill()}
      ctx.save();ctx.translate(n.x,n.y);ctx.rotate(n.rot+Math.sin(t*.001+n.ph)*.03);ctx.scale(n.sc,n.sc);
      const s=56,bg=ctx.createLinearGradient(-s/2,-s/2,s/2,s/2);bg.addColorStop(0,rgba(n.c,.55));bg.addColorStop(1,rgba(n.c,.2));
      ctx.shadowColor=rgba(n.c,.95);ctx.shadowBlur=hover==i?34:20;rr(-s/2,-s/2,s,s,10);ctx.fillStyle=bg;ctx.fill();
      ctx.shadowBlur=0;ctx.lineWidth=hover==i?2.4:1.6;ctx.strokeStyle=rgba(n.c,.95);ctx.stroke();
      ctx.rotate(-n.rot);ctx.fillStyle='#fff';ctx.font='700 13px Segoe UI, Inter, sans-serif';ctx.textAlign='center';ctx.textBaseline='middle';ctx.fillText('#'+n.b.index,0,1);
      ctx.restore()});
    VZ.raf=requestAnimationFrame(frame)
  }
  function pos(e){const r=cv.getBoundingClientRect();return[e.clientX-r.left,e.clientY-r.top]}
  function pick(x,y){let h=-1;nd.forEach((n,i)=>{if(Math.hypot(n.x-x,n.y-y)<38)h=i});return h}
  cv.addEventListener('mousemove',e=>{[mx,my]=pos(e);hover=pick(mx,my);cv.style.cursor=hover>=0?'pointer':'default';
    if(hover>=0){const b=nd[hover].b;tip.innerHTML=`<b>Block #${b.index}</b> · ${esc(b.action)}<br><span style="color:#8b95b5">${esc(b.title||'Untitled')}</span><br><span style="color:#2edb7b">click to view data</span>`;
      tip.style.left=(e.clientX+16)+'px';tip.style.top=(e.clientY+16)+'px';tip.classList.add('show')}else tip.classList.remove('show')});
  cv.addEventListener('mouseleave',()=>{hover=-1;tip.classList.remove('show')});
  cv.addEventListener('click',e=>{const [x,y]=pos(e),h=pick(x,y);if(h>=0)openBlock(nd[h].b.index)});
  VZ={raf:0,rs:()=>build()};addEventListener('resize',VZ.rs);
  build();if(N>5)wrap.scrollLeft=wrap.scrollWidth;
  VZ.raf=requestAnimationFrame(frame);
}

/* ---------- BLOCK DETAIL MODAL (with data preview) ---------- */
function openBlock(i){
  const b=S.chain[i];if(!b){toast('Block not found');return}
  const isData=b.action=='ADD';
  $('#blockModalBody').innerHTML=`
    <h2>Block #${b.index}</h2><div class="m-sub">${esc(b.title)||'Untitled'}</div>
    <div class="row"><span class="k">Action</span><span class="v"><span class="tag ${b.action=='REMOVE'?'bad':''}">${b.action}</span></span></div>
    <div class="row"><span class="k">Category</span><span class="v">${esc(b.category)}</span></div>
    <div class="row"><span class="k">Filename</span><span class="v">${esc(b.filename||'-')}</span></div>
    <div class="row"><span class="k">MIME Type</span><span class="v">${esc(b.mime||'-')}</span></div>
    <div class="row"><span class="k">Size</span><span class="v">${fmt(b.size)}</span></div>
    <div class="row"><span class="k">Time</span><span class="v">${dt(b.time)}</span></div>
    <div class="row"><span class="k">Nonce</span><span class="v">${b.nonce}</span></div>
    <div class="row"><span class="k">Ref</span><span class="v">${b.ref==null?'-':b.ref}</span></div>
    <div class="row"><span class="k">Hash</span><span class="v">${b.hash}</span></div>
    <div class="row"><span class="k">Prev Hash</span><span class="v">${b.prev}</span></div>
    <div class="row"><span class="k">Content Hash</span><span class="v">${b.content_hash}</span></div>
    ${isData?'<div class="pv" id="pv">Loading data...</div>':''}
    <div class="m-btns">${isData?`<button class="btn" onclick="dl(${b.index})">Download</button>`:''}<button class="btn" onclick="closeBlock()">Close</button></div>`;
  $('#blockModal').classList.add('show');
  if(isData)preview(b)
}
async function preview(b){
  const pv=$('#pv'),m=(b.mime||'').toLowerCase();
  try{
    if(m.startsWith('image/')){pv.innerHTML=`<img src="/api/download?id=${b.index}" alt="">`;return}
    if(!(m.startsWith('text/')||/json|xml|csv|javascript/.test(m))){pv.textContent='Binary file ('+b.mime+') - use Download to open it.';return}
    if(b.size>300000){pv.textContent='File is large - use Download.';return}
    const txt=await (await fetch('/api/download?id='+b.index)).text();
    try{const j=JSON.parse(txt);
      if(j&&j.image){pv.innerHTML=`<img src="${esc(j.image)}" alt=""><b>${esc(j.name||'')}</b>\n${esc(j.description||'')}`;return}
      pv.textContent=JSON.stringify(j,null,2)}
    catch(e){pv.textContent=txt}
  }catch(e){pv.textContent='Could not load data.'}
}
function closeBlock(){$('#blockModal').classList.remove('show')}
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeBlock()});

const V={
overview(){const ok=S.nodes.every(n=>n.valid),r=recs();return `<h1>Overview</h1><div class="sub">Decentralized & distributed ledger · store, download, remove, add</div>
<div class="grid">
<div class="card stat"><b>${S.chain.length}</b><span>Total Blocks</span></div>
<div class="card stat"><b>${r.length}</b><span>Active Records</span></div>
<div class="card stat"><b>${recs('data').length}</b><span>Data</span></div>
<div class="card stat"><b>${recs('coin').length}</b><span>Coin Records</span></div>
<div class="card stat"><b>${recs('nft').length}</b><span>NFT Records</span></div>
<div class="card stat"><b style="${ok?'':'color:#ff4d6d'}">${ok?'VALID':'ALERT'}</b><span>Network Health</span></div></div>
<div class="card"><h3 style="margin-top:0;color:#f95c8b;font-weight:600;">Latest Blocks</h3>${S.chain.slice(-5).reverse().map(b=>`<div class="blk"><b>#${b.index}</b> <span class="tag">${b.action}</span> <span class="tag">${b.category}</span> ${esc(b.title)}<div class="hash">${b.hash}</div></div>`).join('')}</div>`},
vault(){return `<h1>Data Vault</h1><div class="sub">Store any text or file in the ledger</div><div class="two">
<div class="card"><label>Title</label><input id="t" placeholder="e.g. My certificate"><label>Text data</label><textarea id="x" rows="4" placeholder="Write text here..."></textarea><label>Or choose a file (max 2MB)</label><input id="f" type="file"><button class="btn" onclick="addVault()">+ Add to Ledger</button></div>
<div class="card">${rows(recs('data'))}</div></div>`},
coins(){return `<h1>Coins</h1><div class="sub">Record basic crypto-coin transactions on the ledger</div><div class="two">
<div class="card"><label>Coin symbol</label><input id="cs" value="GRN"><label>Amount</label><input id="ca" type="number" value="10" min="0"><label>From</label><input id="cf" placeholder="wallet A"><label>To</label><input id="ct" placeholder="wallet B"><button class="btn" onclick="addCoin()">+ Record Transaction</button></div>
<div class="card">${rows(recs('coin'))}</div></div>`},
nfts(){const r=recs('nft');return `<h1>NFTs</h1><div class="sub">Store image + metadata as a unique token (token id = block #)</div>
<div class="card"><div style="display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));"><div><label>NFT name</label><input id="nn"></div><div><label>Description</label><input id="nd"></div><div><label>Image (max 1MB)</label><input id="ni" type="file" accept="image/*"></div></div><button class="btn" onclick="addNft()">+ Mint NFT Record</button></div>
${r.length?`<div class="nft">${r.map(b=>`<div class="card"><img id="im${b.index}" alt=""><b>${esc(b.title)}</b><div class="sub" style="margin:2px 0">Token #${b.index}</div><div class="hash">${b.hash.slice(0,18)}...</div><div style="margin-top:8px"><button class="btn sm" onclick="dl(${b.index})">Download</button> <button class="btn sm red" onclick="rm(${b.index})">Remove</button></div></div>`).join('')}</div>`:'<p class="sub">No NFTs yet.</p>'}`},
explorer(){return `<h1>Ledger Explorer</h1><div class="sub">Full chain · difficulty ${S.diff} (hash starts with "${'0'.repeat(S.diff)}")</div><div class="card">${S.chain.slice().reverse().map(b=>`<div class="blk"><b>#${b.index}</b> <span class="tag">${b.action}</span> <span class="tag">${b.category}</span> ${esc(b.title)} <span style="color:var(--text-muted);font-size:12px">${dt(b.time)} | nonce ${b.nonce} | ${fmt(b.size)}</span><div class="hash">hash: ${b.hash}</div><div class="hash" style="opacity:.6">prev: ${b.prev}</div></div>`).join('')}</div>`},
visual: visual,
nodes(){return `<h1>Network Nodes</h1><div class="sub">Each node holds its own copy of the ledger. Tamper with one node to test consensus.</div>
<div class="grid">${S.nodes.map(n=>`<div class="card"><b style="color:#f95c8b;">${n.name}</b> <span class="tag ${n.valid?'':'bad'}">${n.valid?'VALID':'TAMPERED'}</span><p class="sub" style="margin:8px 0">Blocks: ${n.length}</p><div class="hash">${n.head.slice(0,24)}...</div><br><button class="btn sm red" onclick="act('tamper',{node:'${n.name}'})">Tamper</button></div>`).join('')}</div>
<button class="btn" onclick="act('sync',{})">Run Consensus / Sync</button> <button class="btn" onclick="load();toast('Validated')">Validate All</button>`}};

function draw(){
  stopVisual();
  $('#nav').innerHTML=Object.entries(VIEWS).map(([k,v])=>`<button class="${k==view?'on':''}" onclick="go('${k}')">${v}</button>`).join('');
  const y=scrollY;$('#main').innerHTML=V[view]();scrollTo(0,y);
  if(view=='visual')initVisual();
  if(view=='nfts')recs('nft').forEach(async b=>{try{const r=await fetch('/api/download?id='+b.index);const j=await r.json();const el=$('#im'+b.index);if(el)el.src=j.image}catch(e){}});
}
function go(v){view=v;draw()}
async function addVault(){
  const t=$('#t').value.trim(),f=$('#f').files[0],x=$('#x').value;let d,m='text/plain',fn=t+'.txt';
  try{if(f){d=(await readFile(f,2097152)).split(',')[1];m=f.type||'application/octet-stream';fn=f.name}else d=b64(x)}catch(e){return toast(e)}
  if(!t||(!f&&!x))return toast('Title and data/file are required');
  act('add',{category:'data',title:t,content:d,mime:m,filename:fn})}
function addCoin(){
  const o={symbol:$('#cs').value,amount:+$('#ca').value,from:$('#cf').value,to:$('#ct').value,time:Date.now()};
  if(!o.from||!o.to)return toast('From and To are required');
  act('add',{category:'coin',title:`${o.symbol}: ${o.from} → ${o.to} (${o.amount})`,content:b64(JSON.stringify(o,null,2)),mime:'application/json',filename:'coin-tx.json'})}
async function addNft(){
  const n=$('#nn').value.trim(),f=$('#ni').files[0];if(!n||!f)return toast('Name and image are required');
  try{const img=await readFile(f,1048576);act('add',{category:'nft',title:n,content:b64(JSON.stringify({name:n,description:$('#nd').value,image:img})),mime:'application/json',filename:n+'-nft.json'})}catch(e){toast(e)}}
load();
</script></body></html>'''

if __name__ == "__main__":
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    load()
    port = int(os.environ.get("PORT", 8000))
    on_render = bool(os.environ.get("PORT") or os.environ.get("RENDER"))
    host = "0.0.0.0" if on_render else "127.0.0.1"
    print(f"GREEN LEDGER running at -> http://{host}:{port}  (press Ctrl+C to stop)", flush=True)
    if not on_render:
        threading.Timer(1, lambda: webbrowser.open(f"http://127.0.0.1:{port}")).start()
    try:
        ThreadingHTTPServer((host, port), H).serve_forever()
    except KeyboardInterrupt:
        print("\nBye!")
