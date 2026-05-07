"""src/lineage.py — Lineage data loading + simple click-to-explore vis.js graph.

Design: high-level table nodes only. Click a node → neighbours light up + info
card appears. No 3-pane layout that breaks in Streamlit iframes.
Data injected via .replace("__DATA__", json) — no f-string, no escaping needed.
"""

import json
import streamlit as st
from src import graphdb


@st.cache_data(show_spinner=False, ttl=300)
def load_full(_token: str) -> dict:
    """Load nodes + edges + column/KPI counts from GraphDB."""
    node_rows = graphdb.query("""
        SELECT DISTINCT ?tname ?layer ?platform ?domain WHERE {
            ?t biz:tableName ?tname .
            OPTIONAL { ?t biz:tableLayer ?layer }
            OPTIONAL { ?t biz:sourceSystemType ?platform }
            OPTIONAL { ?t biz:tableDomain ?domain }
        }""", _token) or []

    edge_rows = graphdb.query("""
        SELECT DISTINCT ?srcName ?tgtName ?transformType WHERE {
            ?src biz:feedsInto ?tgt .
            ?src biz:tableName ?srcName .
            ?tgt biz:tableName ?tgtName .
            ?edge biz:sourceTable ?src ; biz:targetTable ?tgt .
            OPTIONAL { ?edge biz:lineageTransformType ?transformType }
        }""", _token) or []

    col_rows = graphdb.query("""
        SELECT ?tname (COUNT(?col) AS ?ncols) (SUM(IF(?pii,1,0)) AS ?npii)
               (SUM(IF(?kpi,1,0)) AS ?nkpi) WHERE {
            ?t biz:tableName ?tname ; biz:hasColumn ?col .
            ?col biz:columnName ?cname .
            OPTIONAL { ?col biz:isPII ?pii }
            OPTIONAL { ?col biz:isKPIColumn ?kpi }
        } GROUP BY ?tname""", _token) or []

    kpi_rows = graphdb.query("""
        SELECT ?tname (COUNT(?k) AS ?nkpis) WHERE {
            ?t biz:tableName ?tname ; biz:hasKPI ?k .
        } GROUP BY ?tname""", _token) or []

    # Build lookup dicts
    col_info = {r["tname"]: {"ncols": int(r.get("ncols",0) or 0),
                              "npii":  int(r.get("npii",0)  or 0),
                              "nkpi":  int(r.get("nkpi",0)  or 0)}
                for r in col_rows}
    kpi_info = {r["tname"]: int(r.get("nkpis",0) or 0) for r in kpi_rows}

    degree = {}
    for e in edge_rows:
        for k in ("srcName","tgtName"):
            n = e.get(k,"")
            degree[n] = degree.get(n,0) + 1

    nodes = []
    for r in node_rows:
        tn = r.get("tname","")
        if not tn: continue
        ci = col_info.get(tn, {})
        nodes.append({
            "id":       tn,
            "layer":    (r.get("layer")    or "").lower(),
            "platform": (r.get("platform") or "").lower(),
            "domain":   (r.get("domain")   or "").capitalize(),
            "degree":   degree.get(tn, 0),
            "ncols":    ci.get("ncols", 0),
            "npii":     ci.get("npii",  0),
            "nkpi":     ci.get("nkpi",  0),
            "nkpis":    kpi_info.get(tn, 0),
        })

    edges = [{"from": r.get("srcName",""), "to": r.get("tgtName",""),
              "type": (r.get("transformType") or "lineage").lower()}
             for r in edge_rows if r.get("srcName") and r.get("tgtName")]

    return {
        "nodes":     nodes,
        "edges":     edges,
        "layers":    sorted({n["layer"]    for n in nodes if n["layer"]}),
        "platforms": sorted({n["platform"] for n in nodes if n["platform"]}),
        "domains":   sorted({n["domain"]   for n in nodes if n["domain"]}),
    }


# ─── HTML Template ────────────────────────────────────────────────────────────
# Plain raw string — __DATA__ replaced at runtime, no f-string escaping needed.

_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<script src="https://cdn.jsdelivr.net/npm/vis-network@9.1.9/standalone/umd/vis-network.min.js"></script>
<style>
* { box-sizing:border-box; margin:0; padding:0; }
body { background:#0d1117; color:#c9d1d9; font-family:'Segoe UI',system-ui,sans-serif;
       font-size:13px; height:780px; overflow:hidden; display:flex; flex-direction:column; }

/* toolbar */
#bar { background:#161b22; border-bottom:1px solid #30363d; padding:8px 12px;
       display:flex; align-items:center; gap:8px; flex-shrink:0; flex-wrap:wrap; }
#srch { background:#0d1117; border:1px solid #30363d; border-radius:6px; color:#c9d1d9;
        padding:4px 10px; width:180px; font-size:12px; outline:none; }
#srch:focus { border-color:#58a6ff; }
.tb { background:#21262d; border:1px solid #30363d; border-radius:6px; color:#8b949e;
      cursor:pointer; padding:4px 9px; font-size:11px; white-space:nowrap; }
.tb:hover { border-color:#58a6ff; color:#58a6ff; }
.tb.on { background:#1f3a5f; border-color:#58a6ff; color:#c9d1d9; }
#info-bar { margin-left:auto; font-size:11px; color:#8b949e; white-space:nowrap; }

/* main area: graph left, card right */
#main { flex:1; display:flex; overflow:hidden; }
#graph-wrap { flex:1; position:relative; }
#net { width:100%; height:100%; }

/* floating zoom controls */
#zoom { position:absolute; bottom:12px; right:12px; display:flex; flex-direction:column; gap:4px; }
.zb { background:#161b22; border:1px solid #30363d; border-radius:6px; color:#8b949e;
      cursor:pointer; padding:5px 8px; font-size:13px; line-height:1; }
.zb:hover { border-color:#58a6ff; color:#58a6ff; }

/* detail card */
#card { width:260px; background:#161b22; border-left:1px solid #30363d;
        display:flex; flex-direction:column; overflow:hidden; transition:width .2s; }
#card.hidden { width:0; border:none; }
#card-inner { padding:14px 12px; overflow-y:auto; flex:1; }
#card-name { font-size:13px; font-weight:700; color:#e6edf3; word-break:break-all; margin-bottom:8px; }
.badge { display:inline-block; padding:2px 7px; border-radius:10px; font-size:10px;
         font-weight:600; margin-right:3px; margin-bottom:4px; }
.b-gold     { background:#2a2100; color:#f9c846; border:1px solid #b8860b; }
.b-silver   { background:#141f2a; color:#79afd1; border:1px solid #37474F; }
.b-bronze   { background:#2d1f0e; color:#e8a55a; border:1px solid #8B4513; }
.b-staging  { background:#1e1228; color:#bc8cff; border:1px solid #4A148C; }
.b-databricks { background:#2a1800; color:#ff8c42; border:1px solid #d1571a; }
.b-snowflake  { background:#0d1e2d; color:#5ab5e8; border:1px solid #1f6feb; }
.b-domain   { background:#21262d; color:#8b949e; border:1px solid #30363d; }
.stat-grid { display:grid; grid-template-columns:1fr 1fr; gap:6px; margin:10px 0; }
.stat { background:#0d1117; border:1px solid #21262d; border-radius:6px;
        padding:8px; text-align:center; }
.stat .n { font-size:18px; font-weight:700; color:#58a6ff; }
.stat .l { font-size:9px; color:#8b949e; text-transform:uppercase; letter-spacing:.05em; }
#neighbours { margin-top:10px; }
.sec { font-size:10px; font-weight:700; letter-spacing:.08em; text-transform:uppercase;
       color:#8b949e; margin:10px 0 5px; }
.nb { display:flex; align-items:center; padding:5px 0; border-bottom:1px solid #1c2128;
      cursor:pointer; gap:6px; }
.nb:hover .nb-name { color:#58a6ff; }
.nb-icon { font-size:14px; flex-shrink:0; }
.nb-name { font-size:11px; color:#c9d1d9; font-weight:500; flex:1; word-break:break-all; }
.nb-tag { font-size:9px; background:#21262d; padding:1px 5px; border-radius:6px; color:#8b949e; }
#hint { display:flex; flex-direction:column; align-items:center; justify-content:center;
        height:100%; color:#8b949e; text-align:center; gap:10px; padding:20px; }
#hint .ico { font-size:32px; }
#hint .txt { font-size:11px; line-height:1.7; }

/* legend strip */
#legend { background:#161b22; border-top:1px solid #30363d; padding:6px 12px;
          display:flex; gap:14px; flex-shrink:0; flex-wrap:wrap; }
.leg { display:flex; align-items:center; gap:5px; font-size:10px; color:#8b949e; }
.ld { width:10px; height:10px; flex-shrink:0; }
</style>
</head>
<body>

<div id="bar">
  <input id="srch" placeholder="🔍 Search tables…" oninput="filterGraph()"/>
  <button class="tb on" id="tb-gold"        onclick="toggleLayer('gold')">⭐ Gold</button>
  <button class="tb on" id="tb-silver"      onclick="toggleLayer('silver')">● Silver</button>
  <button class="tb on" id="tb-bronze"      onclick="toggleLayer('bronze')">▼ Bronze</button>
  <button class="tb on" id="tb-staging"     onclick="toggleLayer('staging')">■ Staging</button>
  <button class="tb on" id="tb-databricks"  onclick="togglePlatform('databricks')">🟠 Databricks</button>
  <button class="tb on" id="tb-snowflake"   onclick="togglePlatform('snowflake')">❄️ Snowflake</button>
  <button class="tb" onclick="net.fit({animation:true})">⊞ Fit</button>
  <button class="tb" onclick="resetAll()">↺ Reset</button>
  <span id="info-bar">–</span>
</div>

<div id="main">
  <div id="graph-wrap">
    <div id="net"></div>
    <div id="zoom">
      <button class="zb" onclick="net.moveTo({scale:net.getScale()*1.3})">＋</button>
      <button class="zb" onclick="net.moveTo({scale:net.getScale()*.75})">－</button>
      <button class="zb" id="physbtn" onclick="togglePhysics()" title="Toggle force">⚛</button>
    </div>
  </div>
  <div id="card" class="hidden">
    <div id="card-inner">
      <div id="hint"><div class="ico">🔷</div><div class="txt">Click any node<br/>to explore its neighbours<br/>and metadata.</div></div>
      <div id="det" style="display:none">
        <div id="card-name"></div>
        <div id="card-badges"></div>
        <div class="stat-grid" id="card-stats"></div>
        <div id="neighbours"></div>
      </div>
    </div>
  </div>
</div>

<div id="legend">
  <div class="leg"><div class="ld" style="background:#F9A825;clip-path:polygon(50% 0%,61% 35%,98% 35%,68% 57%,79% 91%,50% 70%,21% 91%,32% 57%,2% 35%,39% 35%)"></div>Gold</div>
  <div class="leg"><div class="ld" style="background:#607D8B;border-radius:50%"></div>Silver dim</div>
  <div class="leg"><div class="ld" style="background:#607D8B;transform:rotate(45deg)"></div>Silver fact</div>
  <div class="leg"><div class="ld" style="background:#A0522D;clip-path:polygon(50% 0%,0% 100%,100% 100%)"></div>Bronze</div>
  <div class="leg"><div class="ld" style="background:#FF7043;border-radius:50%"></div>🟠 Databricks border</div>
  <div class="leg"><div class="ld" style="background:#29B5E8;border-radius:50%"></div>❄️ Snowflake border</div>
  <div class="leg" style="color:#58a6ff">Node size ∝ connections</div>
</div>

<script>
const DATA = __DATA__;

// ── Build lookups ──────────────────────────────────────────────────────────
const NM = {};
DATA.nodes.forEach(n => NM[n.id] = n);

const UP = {};   // id -> [{id, type}]
const DN = {};
DATA.edges.forEach(e => {
  if (!UP[e.to])   UP[e.to]   = [];
  if (!DN[e.from]) DN[e.from] = [];
  UP[e.to].push({id: e.from, type: e.type});
  DN[e.from].push({id: e.to,   type: e.type});
});

// ── Filter state ───────────────────────────────────────────────────────────
const AL = new Set(DATA.layers);
const AP = new Set(DATA.platforms);
let phys = true;

// ── Visual helpers ─────────────────────────────────────────────────────────
const LC = {
  gold:    {bg:'#F9A825', bd:'#E65100'},
  silver:  {bg:'#607D8B', bd:'#37474F'},
  bronze:  {bg:'#A0522D', bd:'#6B3410'},
  staging: {bg:'#7B1FA2', bd:'#4A148C'},
};
const PB = {databricks:'#FF7043', snowflake:'#29B5E8'};

function shape(n) {
  if (n.layer==='gold')    return 'star';
  if (n.layer==='staging') return 'square';
  if (n.layer==='bronze')  return 'triangleDown';
  const lb = n.id.toLowerCase();
  if (lb.startsWith('fct_')||lb.startsWith('fact_')) return 'diamond';
  return 'ellipse';
}
function vnode(n) {
  const lc = LC[n.layer]||{bg:'#58a6ff',bd:'#1f6feb'};
  const bd = PB[n.platform]||lc.bd;
  const sz = (n.layer==='gold'?22:n.layer==='silver'?16:12) + Math.min(n.degree*3,20);
  const lbl = n.id.length>22 ? n.id.slice(0,20)+'…' : n.id;
  return {
    id: n.id, label: lbl, shape: shape(n), size: sz,
    color: {background:lc.bg, border:bd,
            highlight:{background:'#ffffff',border:bd},
            hover:{background:'#e6edf3',border:bd}},
    font: {size:11, color:'#e6edf3', face:'Segoe UI'},
    title: '<b>'+n.id+'</b><br/>'+n.layer+' | '+n.platform+'<br/>'+n.domain+
           '<br/>'+n.ncols+' cols · '+n.nkpis+' KPIs · '+n.degree+' connections',
  };
}
function vedge(e) {
  const c = {replicate:'#58a6ff',aggregate:'#f9c846',join:'#56d364',transform:'#f0883e',lineage:'#444d56'};
  return {from:e.from, to:e.to, title:e.type,
          color:{color:c[e.type]||'#444d56',highlight:'#ffffff',hover:'#8b949e'},
          width:1.5, arrows:{to:{enabled:true,scaleFactor:.55}},
          smooth:{type:'curvedCW',roundness:.15}};
}

// ── Build network ──────────────────────────────────────────────────────────
const nodesDS = new vis.DataSet(DATA.nodes.map(vnode));
const edgesDS = new vis.DataSet(DATA.edges.map(vedge));
const net = new vis.Network(
  document.getElementById('net'),
  {nodes:nodesDS, edges:edgesDS},
  {
    physics:{enabled:true, barnesHut:{springLength:200,springConstant:.04,damping:.09,gravitationalConstant:-3000}},
    interaction:{hover:true, tooltipDelay:200, hideEdgesOnDrag:true},
    layout:{improvedLayout:true},
  }
);

// ── Filters ────────────────────────────────────────────────────────────────
function visibleIds() {
  const q = document.getElementById('srch').value.toLowerCase();
  return new Set(DATA.nodes.filter(n =>
    (AL.has(n.layer)||!n.layer) &&
    (AP.has(n.platform)||!n.platform) &&
    (!q || n.id.toLowerCase().includes(q))
  ).map(n=>n.id));
}
function filterGraph() {
  const ids = visibleIds();
  DATA.nodes.forEach(n => nodesDS.update({id:n.id, hidden:!ids.has(n.id)}));
  DATA.edges.forEach(e => edgesDS.update({from:e.from,to:e.to,hidden:!ids.has(e.from)||!ids.has(e.to)}));
  updateBar();
}
function updateBar() {
  const v = DATA.nodes.filter(n=>!nodesDS.get(n.id).hidden);
  const ve = DATA.edges.filter(e=>visibleIds().has(e.from)&&visibleIds().has(e.to));
  document.getElementById('info-bar').textContent = v.length+' tables · '+ve.length+' edges';
}
function toggleLayer(l) {
  AL.has(l)?AL.delete(l):AL.add(l);
  document.getElementById('tb-'+l).classList.toggle('on',AL.has(l));
  filterGraph();
}
function togglePlatform(p) {
  AP.has(p)?AP.delete(p):AP.add(p);
  document.getElementById('tb-'+p).classList.toggle('on',AP.has(p));
  filterGraph();
}
function resetAll() {
  DATA.layers.forEach(l=>AL.add(l));
  DATA.platforms.forEach(p=>AP.add(p));
  document.getElementById('srch').value='';
  document.querySelectorAll('.tb').forEach(b=>b.classList.add('on'));
  filterGraph();
  net.fit({animation:true});
  closeCard();
}
function togglePhysics() {
  phys=!phys;
  net.setOptions({physics:{enabled:phys}});
  document.getElementById('physbtn').style.color=phys?'#56d364':'#8b949e';
}

// ── Click → highlight neighbours + show card ──────────────────────────────
net.on('click', p => {
  if (p.nodes.length) {
    const id = p.nodes[0];
    highlightNode(id);
    showCard(id);
  } else {
    resetHighlight();
    closeCard();
  }
});

function highlightNode(id) {
  const neighbours = new Set(net.getConnectedNodes(id));
  neighbours.add(id);
  // dim everything else
  DATA.nodes.forEach(n => {
    const dim = !neighbours.has(n.id);
    nodesDS.update({id:n.id, opacity: dim?0.15:1.0});
  });
  DATA.edges.forEach(e => {
    const connected = neighbours.has(e.from) && neighbours.has(e.to);
    edgesDS.update({from:e.from, to:e.to, color:{
      color: connected ? (e.type==='aggregate'?'#f9c846':e.type==='replicate'?'#58a6ff':'#56d364') : '#1c2128',
      opacity: connected ? 1 : 0.1,
    }});
  });
  net.selectNodes([id]);
}

function resetHighlight() {
  DATA.nodes.forEach(n => nodesDS.update({id:n.id, opacity:1.0}));
  DATA.edges.forEach(e => edgesDS.update({from:e.from, to:e.to,
    color:{color:{replicate:'#58a6ff',aggregate:'#f9c846',join:'#56d364',transform:'#f0883e',lineage:'#444d56'}[e.type]||'#444d56'}}));
  net.unselectAll();
}

// ── Detail card ────────────────────────────────────────────────────────────
function showCard(id) {
  const n = NM[id]; if(!n) return;
  const card = document.getElementById('card');
  card.classList.remove('hidden');
  document.getElementById('hint').style.display='none';
  document.getElementById('det').style.display='block';

  document.getElementById('card-name').textContent = n.id;

  const platBadge = n.platform
    ? '<span class="badge b-'+n.platform+'">'+(n.platform==='databricks'?'🟠':'❄️')+' '+n.platform+'</span>'
    : '';
  document.getElementById('card-badges').innerHTML =
    '<span class="badge b-'+(n.layer||'domain')+'">'+( n.layer||'?').toUpperCase()+'</span>'+
    platBadge +
    (n.domain?'<span class="badge b-domain">'+n.domain+'</span>':'');

  const up = (UP[id]||[]).length, dn = (DN[id]||[]).length;
  document.getElementById('card-stats').innerHTML =
    stat(n.ncols,'Columns')+stat(n.nkpis,'KPIs')+stat(up,'Upstream')+stat(dn,'Downstream');

  let html = '';
  const ups = UP[id]||[], dns = DN[id]||[];
  if (ups.length) {
    html += '<div class="sec">← Upstream</div>';
    html += ups.map(e=>nbRow(e.id,'←',e.type)).join('');
  }
  if (dns.length) {
    html += '<div class="sec">→ Downstream</div>';
    html += dns.map(e=>nbRow(e.id,'→',e.type)).join('');
  }
  if (!ups.length && !dns.length)
    html = '<div style="color:#8b949e;font-size:11px;margin-top:8px">No lineage edges registered.</div>';
  document.getElementById('neighbours').innerHTML = html;
}

function stat(n,l) {
  return '<div class="stat"><div class="n">'+n+'</div><div class="l">'+l+'</div></div>';
}

function nbRow(id, arrow, type) {
  const n = NM[id]||{};
  const ico = n.layer==='gold'?'⭐':n.layer==='silver'?'●':n.layer==='bronze'?'▼':'■';
  return '<div class="nb" onclick="highlightNode(\''+id+'\');showCard(\''+id+'\')">'+
    '<span class="nb-icon">'+ico+'</span>'+
    '<span class="nb-name">'+id+'</span>'+
    '<span class="nb-tag">'+type+'</span>'+
    '</div>';
}

function closeCard() {
  document.getElementById('card').classList.add('hidden');
  document.getElementById('hint').style.display='flex';
  document.getElementById('det').style.display='none';
  resetHighlight();
}

// ── Init ───────────────────────────────────────────────────────────────────
filterGraph();
net.once('stabilized', () => net.fit({animation:true}));
</script>
</body>
</html>"""


def build_rich_html(data: dict) -> str:
    return _TEMPLATE.replace("__DATA__", json.dumps(data, ensure_ascii=False))
