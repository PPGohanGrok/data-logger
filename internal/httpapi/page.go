package httpapi

import (
	"html/template"
	"net/http"
)

const pageHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<title>当前测点</title>
<style>
  :root {
    color-scheme: dark;
    --bg: #12140f;
    --ink: #e7ecdf;
    --muted: #8b947c;
    --line: #2a3124;
    --card: #1a1e16;
    --live: #d6f07a;
    --stale: #e2a84b;
    --bad: #d97a6a;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    min-height: 100vh;
    background: var(--bg);
    color: var(--ink);
    font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  }
  header {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    align-items: flex-end;
    padding: 22px 24px 16px;
    border-bottom: 1px solid var(--line);
  }
  .eyebrow {
    margin: 0 0 4px;
    color: var(--muted);
    font-size: 12px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  h1 { margin: 0; font-size: 28px; font-weight: 600; }
  .sub { margin: 6px 0 0; color: var(--muted); font-size: 14px; }
  .status {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 12px;
    border: 1px solid var(--line);
    background: var(--card);
  }
  .lamp {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: var(--muted);
  }
  body.live .lamp { background: var(--live); }
  body.stale .lamp { background: var(--stale); }
  body.down .lamp { background: var(--bad); }
  #statusText { font-size: 14px; }
  time { color: var(--muted); font-variant-numeric: tabular-nums; font-size: 13px; }
  #banner {
    margin: 0;
    padding: 10px 24px;
    background: #3a2e14;
    color: #f3d7a1;
    border-bottom: 1px solid #6a5424;
  }
  #banner[hidden] { display: none; }
  main {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    background: var(--bg);
    border-top: 1px solid var(--line);
  }
  .cell {
    background: var(--card);
    padding: 14px 14px 12px;
    min-height: 84px;
    border-right: 1px solid var(--line);
    border-bottom: 1px solid var(--line);
  }
  .name {
    margin: 0;
    color: var(--muted);
    font-size: 12px;
    word-break: break-all;
  }
  .value {
    margin: 8px 0 0;
    font-family: "Cascadia Mono", "Sarasa Mono SC", Consolas, ui-monospace, monospace;
    font-size: 22px;
    font-variant-numeric: tabular-nums;
    letter-spacing: -0.03em;
  }
  body.stale .value { color: #cbb892; }
  @media (max-width: 640px) {
    header { flex-direction: column; align-items: flex-start; padding: 16px; }
    h1 { font-size: 22px; }
    .value { font-size: 20px; }
  }
</style>
</head>
<body>
<header>
  <div>
    <p class="eyebrow">时序存储</p>
    <h1>当前测点</h1>
    <p class="sub" id="summary"></p>
  </div>
  <div class="status">
    <span class="lamp" id="lamp"></span>
    <span id="statusText">正在连接</span>
    <time id="sampleTime">—</time>
  </div>
</header>
<p id="banner" hidden>已超过 2 秒没有新样本</p>
<main id="grid"></main>
<script>
const FIELDS = {{.Fields}};
const grid = document.getElementById("grid");
const statusText = document.getElementById("statusText");
const sampleTime = document.getElementById("sampleTime");
const banner = document.getElementById("banner");
const summary = document.getElementById("summary");
const values = new Map();
summary.textContent = FIELDS.length + " 个通道，新样本到达后立即刷新";

function formatValue(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const abs = Math.abs(v);
  if (abs !== 0 && (abs >= 1e6 || abs < 1e-3)) return v.toExponential(3);
  return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 4 }).format(v);
}

FIELDS.forEach(function (name) {
  const cell = document.createElement("article");
  cell.className = "cell";
  const label = document.createElement("p");
  label.className = "name";
  label.textContent = name;
  const num = document.createElement("p");
  num.className = "value";
  num.textContent = "—";
  cell.appendChild(label);
  cell.appendChild(num);
  grid.appendChild(cell);
  values.set(name, num);
});

let staleTimer = 0;
let seenSample = false;
function markLive() {
  document.body.classList.remove("stale", "down");
  document.body.classList.add("live");
  statusText.textContent = "实时";
  banner.hidden = true;
  window.clearTimeout(staleTimer);
  staleTimer = window.setTimeout(markStale, 2500);
}
function markStale() {
  document.body.classList.remove("live");
  document.body.classList.add("stale");
  statusText.textContent = "停滞";
  banner.hidden = false;
}
function show(data) {
  if (!data || !data.ts) {
    sampleTime.textContent = "尚无数据";
    statusText.textContent = "等待样本";
    return;
  }
  seenSample = true;
  const when = new Date(data.ts);
  sampleTime.dateTime = data.ts;
  sampleTime.textContent = when.toLocaleString("zh-CN", { hour12: false });
  const list = data.values || [];
  FIELDS.forEach(function (name, i) {
    const node = values.get(name);
    if (node) node.textContent = formatValue(list[i]);
  });
  markLive();
}

const source = new EventSource("/v1/live");
source.addEventListener("snapshot", function (ev) {
  show(JSON.parse(ev.data));
});
source.addEventListener("sample", function (ev) {
  show(JSON.parse(ev.data));
});
source.addEventListener("heartbeat", function () {
  if (seenSample) markStale();
});
source.onerror = function () {
  document.body.classList.remove("live");
  document.body.classList.add("down");
  statusText.textContent = "正在重连";
};
</script>
</body>
</html>
`

var pageTmpl = template.Must(template.New("live").Parse(pageHTML))

func (s *Server) handlePage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	if err := pageTmpl.Execute(w, s.pageData()); err != nil {
		http.Error(w, "页面生成失败", http.StatusInternalServerError)
	}
}
