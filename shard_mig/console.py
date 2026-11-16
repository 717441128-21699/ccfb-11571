"""迁移控制台：标准库 ThreadingHTTPServer + 内嵌单页 UI。

页面分区对应需求：分片规则、迁移计划、数据搬运、增量追平、切换与回滚。
所有操作走 JSON API，状态每秒自动刷新。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .gateway import Gateway
from .migrator import Migrator
from .sharding import (
    NEW_SHARD_COUNT,
    OLD_SHARD_COUNT,
    expansion_map,
    locate_both,
)

PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>分片在线扩容迁移控制台</title>
<style>
  :root { --bg:#0f172a; --card:#1e293b; --line:#334155; --txt:#e2e8f0;
          --muted:#94a3b8; --accent:#38bdf8; --ok:#34d399; --warn:#fbbf24; --err:#f87171; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--txt);
         font:14px/1.5 -apple-system,"Segoe UI","Microsoft YaHei",sans-serif; }
  header { padding:14px 22px; background:var(--card); border-bottom:1px solid var(--line);
           display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
  header h1 { font-size:17px; margin:0; }
  .badge { padding:3px 10px; border-radius:12px; font-size:12px; background:#0ea5e9; color:#082f49; font-weight:700; }
  .badge.ro { background:var(--warn); color:#422006; }
  .badge.old { background:#a78bfa; color:#2e1065; }
  .badge.err { background:var(--err); color:#450a0a; }
  main { padding:18px 22px; display:grid; gap:16px;
         grid-template-columns:repeat(auto-fit,minmax(360px,1fr)); }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }
  .card h2 { font-size:14px; margin:0 0 10px; color:var(--accent); }
  table { width:100%; border-collapse:collapse; font-size:12.5px; }
  th,td { text-align:left; padding:4px 8px; border-bottom:1px solid var(--line); }
  th { color:var(--muted); font-weight:600; }
  .kv { display:grid; grid-template-columns:130px 1fr; gap:4px 10px; font-size:12.5px; }
  .kv b { color:var(--muted); font-weight:500; }
  button { background:var(--accent); color:#082f49; border:0; border-radius:6px;
           padding:7px 14px; font-weight:700; cursor:pointer; margin:3px 4px 3px 0; }
  button.sec { background:#475569; color:#fff; }
  button.danger { background:var(--err); color:#450a0a; }
  button:disabled { opacity:.4; cursor:not-allowed; }
  input { background:#0f172a; border:1px solid var(--line); color:var(--txt);
          border-radius:6px; padding:6px 9px; width:120px; }
  .bar { height:8px; background:#0f172a; border-radius:5px; overflow:hidden; margin:6px 0; }
  .bar i { display:block; height:100%; background:var(--ok); transition:width .4s; }
  .muted { color:var(--muted); }
  .ok { color:var(--ok); } .warn { color:var(--warn); } .err { color:var(--err); }
  #events { max-height:210px; overflow:auto; font-family:ui-monospace,Consolas,monospace; font-size:12px; }
  #events div { padding:2px 0; border-bottom:1px dashed var(--line); }
  .seg-wrap { max-height:230px; overflow:auto; }
  .wide { grid-column:1/-1; }
</style>
</head>
<body>
<header>
  <h1>分片在线扩容迁移控制台</h1>
  <span id="phase" class="badge">--</span>
  <span id="gmode" class="badge">--</span>
  <span class="muted" id="gseq"></span>
  <span style="flex:1"></span>
  <span class="muted">4 分片 → 16 分片 · SQLite 模拟 · 自动刷新 1s</span>
</header>
<main>
  <section class="card">
    <h2>① 分片规则</h2>
    <div class="kv">
      <b>旧规则</b><span>shard = uid % <span id="oc"></span></span>
      <b>新规则</b><span>shard = uid % <span id="nc"></span></span>
    </div>
    <div style="margin:8px 0">
      <input id="uidInput" type="number" placeholder="输入用户ID" style="width:140px">
      <button onclick="locate()">计算位置</button>
    </div>
    <div id="loc" class="muted"></div>
    <table style="margin-top:8px"><thead><tr><th>旧分片</th><th>拆分到新分片</th></tr></thead>
      <tbody id="emap"></tbody></table>
  </section>

  <section class="card">
    <h2>② 迁移计划</h2>
    <div class="kv" id="plan"></div>
    <div style="margin-top:10px">
      限速 <input id="rate" type="number" min="0"> 行/秒（0=不限）
      <button class="sec" onclick="setRate()">应用</button>
    </div>
  </section>

  <section class="card">
    <h2>③ 数据搬运（存量分批 + 限速）</h2>
    <div class="bar"><i id="bfbar" style="width:0%"></i></div>
    <div class="kv" id="backfill"></div>
    <div style="margin-top:8px">
      <button onclick="act('start')">启动迁移</button>
      <button class="sec" onclick="act('pause')">暂停</button>
      <button class="sec" onclick="act('resume')">恢复</button>
    </div>
  </section>

  <section class="card">
    <h2>④ 增量追平（变更日志 → 新库）</h2>
    <div class="kv" id="catchup"></div>
    <div style="margin-top:8px">
      <button class="sec" onclick="act('verify')">分段校验和 + 重搬</button>
    </div>
    <div class="kv" id="verify" style="margin-top:8px"></div>
  </section>

  <section class="card">
    <h2>⑤ 切换与回滚</h2>
    <div class="kv" id="rollback"></div>
    <div style="margin-top:8px">
      <button onclick="act('switch')">切换到新分片（秒级只读）</button>
      <button class="danger" onclick="doRollback()">一键回滚到旧分片</button>
      <button class="sec" onclick="act('reswitch')">回滚后重新切换</button>
    </div>
    <p class="muted" style="font-size:12px">切换前自动最终追平 + 手机号重复终检；
       回滚把切换后的新写入按 gseq 序反向回放，数据不丢。</p>
  </section>

  <section class="card">
    <h2>数据量对照</h2>
    <div class="kv" id="counts"></div>
    <div class="kv" id="gwstats" style="margin-top:8px"></div>
  </section>

  <section class="card wide">
    <h2>校验分段明细（按主键分段，逐段比对 SHA-256）</h2>
    <div class="seg-wrap"><table>
      <thead><tr><th>#</th><th>主键区间</th><th>行数</th><th>旧库 hash</th><th>新库 hash</th><th>结果</th></tr></thead>
      <tbody id="segs"></tbody></table></div>
  </section>

  <section class="card wide">
    <h2>事件日志</h2>
    <div id="events"></div>
  </section>
</main>
<script>
const $ = id => document.getElementById(id);
async function api(path, opts) {
  const r = await fetch(path, opts);
  const j = await r.json();
  if (!j.ok) { alert(j.error || '操作失败'); throw new Error(j.error); }
  return j.data;
}
const post = (path, body) => api(path, {method:'POST',
  headers:{'Content-Type':'application/json'}, body: JSON.stringify(body||{})});
const act = a => post('/api/action', {action:a});
function setRate() { post('/api/rate', {rate: parseInt($('rate').value)||0}); }
function doRollback() { if (confirm('确认回滚到旧分片？切换后的新写入将反向回放，不丢失。')) act('rollback'); }
async function locate() {
  const uid = parseInt($('uidInput').value);
  if (Number.isNaN(uid)) return;
  const d = await post('/api/locate', {user_id: uid});
  $('loc').innerHTML = `旧库: <b>${d.old.db_file}</b> (shard ${d.old.shard})<br>`
    + `新库: <b>${d.new.db_file}</b> (shard ${d.new.shard})`;
}
function kv(el, obj) {
  el.innerHTML = Object.entries(obj).map(([k,v]) =>
    `<b>${k}</b><span>${typeof v==='object'?JSON.stringify(v):v}</span>`).join('');
}
async function refresh() {
  const s = await api('/api/status');
  const ph = $('phase'); ph.textContent = s.phase;
  ph.className = 'badge ' + (s.phase==='FAILED'?'err':s.gateway_mode==='readonly'?'ro':s.gateway_mode==='old'?'old':'');
  $('gmode').textContent = '网关: ' + s.gateway_mode.toUpperCase()
    + (s.gateway_mode==='readonly' ? '（读→'+s.ro_target+'）' : '');
  $('gmode').className = 'badge ' + (s.gateway_mode==='readonly'?'ro':s.gateway_mode==='old'?'old':'');
  $('gseq').textContent = '全局 gseq: ' + s.current_gseq;
  $('oc').textContent = s.plan.old_shard_count;
  $('nc').textContent = s.plan.new_shard_count;
  kv($('plan'), s.plan);
  const total = s.counts.control_users || 1;
  const pct = Math.min(100, 100 * s.backfill.rows_copied / total);
  $('bfbar').style.width = pct + '%';
  kv($('backfill'), s.backfill);
  kv($('catchup'), s.catchup);
  kv($('verify'), s.verify);
  kv($('rollback'), s.rollback);
  kv($('counts'), s.counts);
  $('segs').innerHTML = (s.segments||[]).map(x =>
    `<tr><td>${x.index}</td><td>${x.id_start} ~ ${x.id_end}</td><td>${x.rows}</td>
     <td style="font-family:monospace">${x.old_hash.slice(0,12)}</td>
     <td style="font-family:monospace">${x.new_hash.slice(0,12)}</td>
     <td class="${x.match?'ok':'err'}">${x.match?'一致':'不一致'}</td></tr>`).join('')
     || '<tr><td colspan="6" class="muted">尚未校验</td></tr>';
  $('events').innerHTML = s.events.map(e =>
    `<div><span class="muted">${e.ts}</span> [${e.phase}] ${e.message}</div>`).join('');
}
fetch('/api/rules').then(r=>r.json()).then(d => {
  $('emap').innerHTML = d.data.expansion_map.map(m =>
    `<tr><td>old_shard_${String(m.old_shard).padStart(2,'0')}</td>
     <td>${m.new_shards.map(n=>'new_shard_'+String(n).padStart(2,'0')).join(', ')}</td></tr>`).join('');
});
refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
"""


class ConsoleServer:
    def __init__(self, gateway: Gateway, migrator: Migrator, host: str, port: int):
        self.gw = gateway
        self.mg = migrator
        self.host = host
        self.port = port
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        gw, mg = self.gw, self.mg

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # 静默默认访问日志
                pass

            def _send(self, payload: dict, code: int = 200) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _read_body(self) -> dict:
                length = int(self.headers.get("Content-Length", 0))
                if not length:
                    return {}
                return json.loads(self.rfile.read(length).decode("utf-8"))

            def do_GET(self):
                if self.path == "/" or self.path == "/index.html":
                    body = PAGE.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path == "/api/status":
                    self._send({"ok": True, "data": mg.status()})
                elif self.path == "/api/rules":
                    self._send({
                        "ok": True,
                        "data": {
                            "old_shard_count": OLD_SHARD_COUNT,
                            "new_shard_count": NEW_SHARD_COUNT,
                            "expansion_map": expansion_map(),
                        },
                    })
                else:
                    self._send({"ok": False, "error": "not found"}, 404)

            def do_POST(self):
                try:
                    body = self._read_body()
                    if self.path == "/api/locate":
                        self._send({"ok": True, "data": locate_both(int(body["user_id"]))})
                    elif self.path == "/api/rate":
                        mg.set_rate(int(body.get("rate", 0)))
                        self._send({"ok": True, "data": {"rate": body["rate"]}})
                    elif self.path == "/api/action":
                        self._send({"ok": True, "data": self._action(body["action"])})
                    else:
                        self._send({"ok": False, "error": "not found"}, 404)
                except Exception as exc:
                    self._send({"ok": False, "error": str(exc)}, 400)

            def _action(self, action: str) -> dict:
                if action == "start":
                    mg.start()
                elif action == "pause":
                    mg.pause()
                elif action == "resume":
                    mg.resume()
                elif action == "verify":
                    return mg.verify_checksum()
                elif action == "switch":
                    return mg.switch_to_new()
                elif action == "rollback":
                    return mg.rollback_to_old()
                elif action == "reswitch":
                    return mg.reswitch_to_new()
                else:
                    raise ValueError(f"未知操作: {action}")
                return {"action": action, "accepted": True}

        class _Server(ThreadingHTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._httpd = _Server((self.host, self.port), Handler)
        self.port = self._httpd.server_address[1]
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name="console-http", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
