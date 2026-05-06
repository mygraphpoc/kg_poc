"""src/lineage.py — Lineage data loading + rich vis.js graph HTML builder.

The HTML is a plain string (not an f-string) to avoid every JS/CSS
curly-brace needing to be escaped. Data is injected with .replace().
"""

import json
import streamlit as st
from src import graphdb


# ─── Data loading ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=300)
def load_full(_token: str) -> dict:
    """Single load of all graph data needed for the rich lineage UI."""
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
        SELECT ?tname ?colName ?isPII ?isKPI WHERE {
            ?t biz:tableName ?tname ; biz:hasColumn ?col .
            ?col biz:columnName ?colName .
            OPTIONAL { ?col biz:isPII ?isPII }
            OPTIONAL { ?col biz:isKPIColumn ?isKPI }
        } ORDER BY ?tname ?colName""", _token) or []

    kpi_rows = graphdb.query("""
        SELECT ?tname ?kpiName ?direction ?benchmark WHERE {
            ?t biz:tableName ?tname ; biz:hasKPI ?kpi .
            ?kpi biz:kpiName ?kpiName .
            OPTIONAL { ?kpi biz:kpiDirection ?direction }
            OPTIONAL { ?kpi biz:kpiBenchmark ?benchmark }
        } ORDER BY ?tname ?kpiName""", _token) or []

    cols_by_table: dict = {}
    for r in col_rows:
        tn = r.get("tname", "")
        cols_by_table.setdefault(tn, []).append({
            "name": r.get("colName", ""),
            "pii":  bool(r.get("isPII")),
            "kpi":  bool(r.get("isKPI")),
        })

    kpis_by_table: dict = {}
    for r in kpi_rows:
        tn = r.get("tname", "")
        kpis_by_table.setdefault(tn, []).append({
            "name":      r.get("kpiName", ""),
            "direction": r.get("direction", ""),
            "benchmark": r.get("benchmark", ""),
        })

    degree: dict = {}
    for e in edge_rows:
        for k in ("srcName", "tgtName"):
            n = e.get(k, "")
            degree[n] = degree.get(n, 0) + 1

    nodes = []
    for r in node_rows:
        tn = r.get("tname", "")
        if not tn:
            continue
        nodes.append({
            "id":       tn,
            "label":    tn,
            "layer":    (r.get("layer")    or "").lower(),
            "platform": (r.get("platform") or "").lower(),
            "domain":   (r.get("domain")   or "").capitalize(),
            "degree":   degree.get(tn, 0),
            "columns":  cols_by_table.get(tn, []),
            "kpis":     kpis_by_table.get(tn, []),
        })

    edges = [
        {
            "from": r.get("srcName", ""),
            "to":   r.get("tgtName", ""),
            "type": (r.get("transformType") or "lineage").lower(),
        }
        for r in edge_rows if r.get("srcName") and r.get("tgtName")
    ]

    return {
        "nodes":     nodes,
        "edges":     edges,
        "layers":    sorted({n["layer"]    for n in nodes if n["layer"]}),
        "platforms": sorted({n["platform"] for n in nodes if n["platform"]}),
        "domains":   sorted({n["domain"]   for n in nodes if n["domain"]}),
    }


# ─── HTML template (plain string — no f-string, no escaping needed) ───────────

# __DATA__ is replaced at runtime with the JSON payload.
_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>Data Lineage</title>
<script src="https://cdn.jsdelivr.net/npm/vis-network@9.1.9/standalone/umd/vis-network.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #0d1117; color: #c9d1d9;
  font-family: 'Segoe UI', system-ui, sans-serif;
  font-size: 13px; height: 820px; overflow: hidden;
}
#shell {
  display: grid;
  grid-template-columns: 230px 1fr 300px;
  grid-template-rows: 48px 1fr;
  height: 820px;
}
#topbar {
  grid-column: 1 / -1;
  background: #161b22; border-bottom: 1px solid #30363d;
  display: flex; align-items: center; gap: 8px; padding: 0 14px;
  flex-wrap: nowrap; overflow-x: auto;
}
#left  { background: #161b22; border-right: 1px solid #30363d; overflow-y: auto; padding: 12px; }
#center { position: relative; overflow: hidden; }
#network { width: 100%; height: 100%; }
#right { background: #161b22; border-left: 1px solid #30363d; overflow-y: auto; }

/* topbar controls */
#search {
  background: #0d1117; border: 1px solid #30363d; border-radius: 6px;
  color: #c9d1d9; padding: 4px 10px; width: 200px; outline: none; font-size: 12px;
}
#search:focus { border-color: #58a6ff; }
.tbtn {
  background: #21262d; border: 1px solid #30363d; border-radius: 6px;
  color: #8b949e; cursor: pointer; padding: 4px 9px; font-size: 11px; white-space: nowrap;
}
.tbtn:hover  { border-color: #58a6ff; color: #58a6ff; }
.tbtn.active { background: #1f6feb30; border-color: #58a6ff; color: #c9d1d9; }
#statsbar { margin-left: auto; font-size: 11px; color: #8b949e; white-space: nowrap; }

/* left panel */
.sec { font-size: 10px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase;
       color: #8b949e; margin: 14px 0 6px; }
.leg { display: flex; align-items: center; gap: 7px; margin-bottom: 6px; font-size: 11px; color: #8b949e; }
.dot { width: 11px; height: 11px; flex-shrink: 0; }
.domain-chips { display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 8px; }
.dchip {
  display: inline-block; padding: 2px 8px; border-radius: 10px; cursor: pointer;
  font-size: 11px; background: #21262d; color: #8b949e; border: 1px solid #30363d;
}
.dchip:hover { border-color: #58a6ff; color: #58a6ff; }
.dchip.off { opacity: .3; }
.srow { display: flex; justify-content: space-between; margin-bottom: 5px; font-size: 11px; }
.sval { color: #58a6ff; font-weight: 600; }

/* graph floating controls */
#gc {
  position: absolute; top: 10px; right: 10px;
  display: flex; flex-direction: column; gap: 5px; z-index: 10;
}
.ibtn {
  background: #161b22; border: 1px solid #30363d; border-radius: 6px;
  color: #8b949e; cursor: pointer; padding: 6px 9px; font-size: 13px; line-height: 1;
}
.ibtn:hover { border-color: #58a6ff; color: #58a6ff; }

/* right detail panel */
#placeholder {
  display: flex; flex-direction: column; align-items: center; justify-content: center;
  height: 100%; color: #8b949e; text-align: center; padding: 30px; gap: 12px;
}
#placeholder .ico { font-size: 36px; }
#placeholder .hint { font-size: 12px; line-height: 1.7; }
#det { display: none; }
#det-hdr { padding: 14px 14px 10px; border-bottom: 1px solid #21262d; }
#det-name { font-size: 14px; font-weight: 700; color: #e6edf3; word-break: break-all; margin-bottom: 8px; }
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 10px;
  font-size: 10px; font-weight: 600; margin-right: 4px; margin-bottom: 4px;
}
.b-gold     { background: #2a2100; color: #f9c846; border: 1px solid #b8860b; }
.b-silver   { background: #141f2a; color: #79afd1; border: 1px solid #37474F; }
.b-bronze   { background: #2d1f0e; color: #e8a55a; border: 1px solid #8B4513; }
.b-staging  { background: #1e1228; color: #bc8cff; border: 1px solid #4A148C; }
.b-databricks { background: #2a1800; color: #ff8c42; border: 1px solid #d1571a; }
.b-snowflake  { background: #0d1e2d; color: #5ab5e8; border: 1px solid #1f6feb; }
.b-domain   { background: #21262d; color: #8b949e; border: 1px solid #30363d; }
#det-mini { display: flex; gap: 6px; padding: 10px 14px; border-bottom: 1px solid #21262d; }
.ms { background: #0d1117; border: 1px solid #21262d; border-radius: 6px; padding: 7px 10px; text-align: center; flex: 1; }
.ms .n { font-size: 16px; font-weight: 700; color: #58a6ff; }
.ms .l { font-size: 9px; color: #8b949e; text-transform: uppercase; letter-spacing: .05em; }
#det-tabs { display: flex; border-bottom: 1px solid #21262d; }
.dtab {
  padding: 8px 12px; cursor: pointer; font-size: 12px; color: #8b949e;
  border-bottom: 2px solid transparent;
}
.dtab:hover { color: #c9d1d9; }
.dtab.on { color: #58a6ff; border-bottom-color: #58a6ff; }
#det-body { padding: 12px; }
.ct { width: 100%; border-collapse: collapse; font-size: 11px; }
.ct th { color: #8b949e; text-align: left; padding: 4px 5px; border-bottom: 1px solid #21262d;
          font-weight: 600; text-transform: uppercase; font-size: 10px; letter-spacing: .05em; }
.ct td { padding: 4px 5px; border-bottom: 1px solid #161b22; color: #c9d1d9; }
.ct tr:hover td { background: #21262d; }
.flag { font-size: 9px; padding: 1px 5px; border-radius: 8px; margin-left: 3px; }
.fpii { background: #5a0a0a; color: #ffa198; }
.fkpi { background: #0a3a1a; color: #56d364; }
.kcard { background: #0d1117; border: 1px solid #21262d; border-radius: 6px; padding: 10px; margin-bottom: 8px; }
.kname { font-size: 12px; font-weight: 600; color: #e6edf3; margin-bottom: 4px; }
.kmeta { font-size: 11px; color: #8b949e; display: flex; gap: 12px; flex-wrap: wrap; }
.larrow {
  display: flex; align-items: center; padding: 7px 0;
  border-bottom: 1px solid #21262d; gap: 8px; cursor: pointer;
}
.larrow:hover .lname { color: #58a6ff; }
.lname { font-size: 12px; font-weight: 500; color: #c9d1d9; }
.lplat { font-size: 10px; color: #8b949e; }
.ltype { font-size: 10px; background: #21262d; padding: 2px 6px; border-radius: 8px; color: #8b949e; margin-left: auto; }
.arr { font-size: 15px; color: #444d56; flex-shrink: 0; }
.srow { display: flex; justify-content: space-between; margin-bottom: 5px; font-size: 11px; }
</style>
</head>
<body>
<div id="shell">

  <div id="topbar">
    <input id="search" placeholder="🔍 Search tables…" oninput="applyFilters()"/>
    <button class="tbtn active" id="btn-gold"        onclick="toggleLayer('gold')"       >⭐ Gold</button>
    <button class="tbtn active" id="btn-silver"      onclick="toggleLayer('silver')"     >● Silver</button>
    <button class="tbtn active" id="btn-bronze"      onclick="toggleLayer('bronze')"     >▼ Bronze</button>
    <button class="tbtn active" id="btn-staging"     onclick="toggleLayer('staging')"    >■ Staging</button>
    <button class="tbtn active" id="btn-databricks"  onclick="togglePlatform('databricks')">🟠 Databricks</button>
    <button class="tbtn active" id="btn-snowflake"   onclick="togglePlatform('snowflake')" >❄️ Snowflake</button>
    <button class="tbtn" onclick="fitAll()" style="margin-left:4px">⊞ Fit</button>
    <button class="tbtn" onclick="resetAll()">↺ Reset</button>
    <span id="statsbar">–</span>
  </div>

  <div id="left">
    <div class="sec">Node shape</div>
    <div class="leg"><div class="dot" style="background:#F9A825;clip-path:polygon(50% 0%,61% 35%,98% 35%,68% 57%,79% 91%,50% 70%,21% 91%,32% 57%,2% 35%,39% 35%)"></div> Gold aggregate</div>
    <div class="leg"><div class="dot" style="background:#607D8B;transform:rotate(45deg)"></div> Silver fact</div>
    <div class="leg"><div class="dot" style="background:#78909C;border-radius:50%"></div> Silver dimension</div>
    <div class="leg"><div class="dot" style="background:#A0522D;clip-path:polygon(50% 100%,0% 0%,100% 0%)"></div> Bronze raw</div>
    <div class="leg"><div class="dot" style="background:#7B1FA2;border-radius:2px"></div> Staging</div>
    <div class="sec">Platform border</div>
    <div class="leg"><div class="dot" style="background:#FF7043;border-radius:50%"></div> Databricks</div>
    <div class="leg"><div class="dot" style="background:#29B5E8;border-radius:50%"></div> Snowflake</div>
    <div class="sec">Domain</div>
    <div class="domain-chips" id="dchips"></div>
    <div class="sec">Visible</div>
    <div id="lstats"></div>
  </div>

  <div id="center">
    <div id="network"></div>
    <div id="gc">
      <button class="ibtn" title="Zoom in"      onclick="network.moveTo({scale: network.getScale()*1.3})">＋</button>
      <button class="ibtn" title="Zoom out"     onclick="network.moveTo({scale: network.getScale()*0.75})">－</button>
      <button class="ibtn" title="Fit all"      onclick="fitAll()">⊞</button>
      <button class="ibtn" title="Toggle force" onclick="togglePhysics()" id="pbtn">⚛</button>
    </div>
  </div>

  <div id="right">
    <div id="placeholder">
      <div class="ico">🔷</div>
      <div class="hint">Click any node to explore<br/>its metadata, columns,<br/>KPIs and lineage.</div>
    </div>
    <div id="det">
      <div id="det-hdr">
        <div id="det-name"></div>
        <div id="det-badges"></div>
      </div>
      <div id="det-mini"></div>
      <div id="det-tabs">
        <div class="dtab on"  onclick="tab('overview')">Overview</div>
        <div class="dtab"     onclick="tab('columns')">Columns</div>
        <div class="dtab"     onclick="tab('kpis')">KPIs</div>
        <div class="dtab"     onclick="tab('lineage')">Lineage</div>
      </div>
      <div id="det-body"></div>
    </div>
  </div>
</div>

<script>
const RAW = __DATA__;

// ── lookups
const NM = {};
RAW.nodes.forEach(n => NM[n.id] = n);
const UP = {};   // nodeId -> [{id, type}]
const DN = {};
RAW.edges.forEach(e => {
  if (!UP[e.to])   UP[e.to]   = [];
  if (!DN[e.from]) DN[e.from] = [];
  UP[e.to].push({id: e.from, type: e.type});
  DN[e.from].push({id: e.to, type: e.type});
});

// ── filter state
const AL = new Set(RAW.layers);
const AP = new Set(RAW.platforms);
const AD = new Set(RAW.domains);
let SEL = null;
let PHYS = true;
let CUR_TAB = 'overview';

// ── node visual helpers
const LC = {
  gold:    {bg:'#F9A825', bd:'#E65100'},
  silver:  {bg:'#607D8B', bd:'#37474F'},
  bronze:  {bg:'#A0522D', bd:'#6B3410'},
  staging: {bg:'#7B1FA2', bd:'#4A148C'},
};
const PB = {databricks:'#FF7043', snowflake:'#29B5E8'};

function nodeShape(n) {
  if (n.layer === 'gold')    return 'star';
  if (n.layer === 'staging') return 'square';
  if (n.layer === 'bronze')  return 'triangleDown';
  const lb = n.id.toLowerCase();
  if (lb.startsWith('fct_') || lb.startsWith('fact_')) return 'diamond';
  return 'ellipse';
}
function nodeSize(n) {
  const base = n.layer === 'gold' ? 22 : n.layer === 'silver' ? 16 : 12;
  return base + Math.min(n.degree * 3, 20);
}
function vn(n) {
  const lc = LC[n.layer] || {bg:'#58a6ff', bd:'#1f6feb'};
  const border = PB[n.platform] || lc.bd;
  const lbl = n.id.length > 22 ? n.id.slice(0,20)+'…' : n.id;
  return {
    id: n.id, label: lbl,
    title: '<b>'+n.id+'</b><br/>Layer: '+n.layer+' | '+n.platform+'<br/>Domain: '+n.domain+'<br/>'+n.columns.length+' cols · '+n.kpis.length+' KPIs',
    shape: nodeShape(n), size: nodeSize(n),
    color: {background:lc.bg, border:border, highlight:{background:'#ffffff',border:border}, hover:{background:'#e6edf3',border:border}},
    font: {size:11, color:'#e6edf3', face:'Segoe UI'},
  };
}
function ve(e) {
  const clr = {replicate:'#58a6ff',aggregate:'#f9c846',join:'#56d364',transform:'#f0883e',lineage:'#444d56'};
  return {
    from:e.from, to:e.to,
    color:{color:clr[e.type]||'#444d56', hover:'#8b949e', highlight:'#ffffff'},
    width:1.5,
    arrows:{to:{enabled:true,scaleFactor:.55}},
    smooth:{type:'curvedCW',roundness:.15},
    title:e.type,
  };
}

// ── DataSets + Network
const nodesDS = new vis.DataSet(RAW.nodes.map(vn));
const edgesDS = new vis.DataSet(RAW.edges.map(ve));
const network = new vis.Network(document.getElementById('network'), {nodes:nodesDS, edges:edgesDS}, {
  physics:{enabled:true, barnesHut:{springLength:180,springConstant:.04,damping:.09,gravitationalConstant:-3000}},
  interaction:{hover:true,tooltipDelay:200},
  layout:{improvedLayout:true},
});

// ── filters
function visible() {
  const q = document.getElementById('search').value.toLowerCase();
  return RAW.nodes.filter(n =>
    (AL.has(n.layer) || !n.layer) &&
    (AP.has(n.platform) || !n.platform) &&
    (AD.size === 0 || AD.has(n.domain)) &&
    (!q || n.id.toLowerCase().includes(q))
  ).map(n => n.id);
}
function applyFilters() {
  const ids = new Set(visible());
  RAW.nodes.forEach(n => nodesDS.update({id:n.id, hidden:!ids.has(n.id)}));
  RAW.edges.forEach(e => edgesDS.update({from:e.from, to:e.to, hidden:!ids.has(e.from)||!ids.has(e.to)}));
  updateStats();
}
function updateStats() {
  const vis2 = RAW.nodes.filter(n => !nodesDS.get(n.id).hidden);
  const vids = new Set(vis2.map(n=>n.id));
  const ve2  = RAW.edges.filter(e=>vids.has(e.from)&&vids.has(e.to));
  document.getElementById('statsbar').textContent = vis2.length+' tables · '+ve2.length+' edges';
  const ls = {};
  vis2.forEach(n=>{ls[n.layer]=(ls[n.layer]||0)+1;});
  document.getElementById('lstats').innerHTML = Object.entries(ls).map(([l,c])=>
    '<div class="srow"><span style="text-transform:capitalize">'+l+'</span><span class="sval">'+c+'</span></div>'
  ).join('');
}
function toggleLayer(l) {
  AL.has(l) ? AL.delete(l) : AL.add(l);
  document.getElementById('btn-'+l).classList.toggle('active', AL.has(l));
  applyFilters();
}
function togglePlatform(p) {
  AP.has(p) ? AP.delete(p) : AP.add(p);
  document.getElementById('btn-'+p).classList.toggle('active', AP.has(p));
  applyFilters();
}
function toggleDomain(d, el) {
  AD.has(d) ? AD.delete(d) : AD.add(d);
  el.classList.toggle('off', !AD.has(d));
  applyFilters();
}
function resetAll() {
  RAW.layers.forEach(l=>AL.add(l));
  RAW.platforms.forEach(p=>AP.add(p));
  RAW.domains.forEach(d=>AD.add(d));
  document.getElementById('search').value='';
  document.querySelectorAll('.tbtn').forEach(b=>b.classList.add('active'));
  document.querySelectorAll('#dchips .dchip').forEach(c=>c.classList.remove('off'));
  applyFilters(); fitAll();
}
function fitAll() { network.fit({animation:{duration:400,easingFunction:'easeInOutQuad'}}); }
function togglePhysics() {
  PHYS = !PHYS;
  network.setOptions({physics:{enabled:PHYS}});
  document.getElementById('pbtn').style.color = PHYS ? '#56d364' : '#8b949e';
}

// ── domain chips
const dc = document.getElementById('dchips');
RAW.domains.forEach(d => {
  const c = document.createElement('span');
  c.className = 'dchip'; c.textContent = d;
  c.onclick = () => toggleDomain(d, c);
  dc.appendChild(c);
});

// ── click handlers
network.on('click', p => {
  if (p.nodes.length > 0) {
    showDetail(p.nodes[0]);
    network.selectNodes([p.nodes[0]].concat(network.getConnectedNodes(p.nodes[0])));
  } else {
    hideDetail();
    network.unselectAll();
  }
});

// ── detail panel
function showDetail(id) {
  SEL = id;
  const n = NM[id]; if(!n) return;
  document.getElementById('placeholder').style.display = 'none';
  document.getElementById('det').style.display = 'block';
  document.getElementById('det-name').textContent = n.id;
  const platBadge = n.platform ? '<span class="badge b-'+n.platform+'">'+(n.platform==='databricks'?'🟠':'❄️')+' '+n.platform+'</span>' : '';
  document.getElementById('det-badges').innerHTML =
    '<span class="badge b-'+n.layer+'">'+n.layer.toUpperCase()+'</span>'+
    platBadge +
    (n.domain ? '<span class="badge b-domain">'+n.domain+'</span>' : '');
  document.getElementById('det-mini').innerHTML =
    ms(n.columns.length,'Cols') + ms(n.kpis.length,'KPIs') +
    ms((UP[id]||[]).length,'Upstream') + ms((DN[id]||[]).length,'Downstream');
  tab(CUR_TAB);
}
function ms(n,l) { return '<div class="ms"><div class="n">'+n+'</div><div class="l">'+l+'</div></div>'; }
function hideDetail() {
  SEL = null;
  document.getElementById('placeholder').style.display = 'flex';
  document.getElementById('det').style.display = 'none';
}

function tab(t) {
  CUR_TAB = t;
  document.querySelectorAll('.dtab').forEach((el,i) => el.classList.toggle('on', ['overview','columns','kpis','lineage'][i]===t));
  if (!SEL) return;
  const n = NM[SEL];
  const body = document.getElementById('det-body');
  if (t==='overview') {
    body.innerHTML =
      '<div class="srow"><span>Layer</span><span class="sval">'+n.layer+'</span></div>'+
      '<div class="srow"><span>Platform</span><span class="sval">'+(n.platform||'—')+'</span></div>'+
      '<div class="srow"><span>Domain</span><span class="sval">'+(n.domain||'—')+'</span></div>'+
      '<div class="srow"><span>Columns</span><span class="sval">'+n.columns.length+'</span></div>'+
      '<div class="srow"><span>KPIs registered</span><span class="sval">'+n.kpis.length+'</span></div>'+
      '<div class="srow"><span>Upstream tables</span><span class="sval">'+((UP[n.id]||[]).length)+'</span></div>'+
      '<div class="srow"><span>Downstream tables</span><span class="sval">'+((DN[n.id]||[]).length)+'</span></div>'+
      '<div class="srow"><span>Total connections</span><span class="sval">'+n.degree+'</span></div>';
  } else if (t==='columns') {
    if (!n.columns.length) { body.innerHTML='<p style="color:#8b949e;font-size:11px">No column metadata in graph.</p>'; return; }
    body.innerHTML = '<table class="ct"><thead><tr><th>Column</th><th>Tags</th></tr></thead><tbody>' +
      n.columns.map(c=>'<tr><td>'+c.name+'</td><td>'+
        (c.pii?'<span class="flag fpii">PII</span>':'')+
        (c.kpi?'<span class="flag fkpi">KPI</span>':'')+'</td></tr>').join('') +
      '</tbody></table>';
  } else if (t==='kpis') {
    if (!n.kpis.length) { body.innerHTML='<p style="color:#8b949e;font-size:11px">No KPIs registered.</p>'; return; }
    body.innerHTML = n.kpis.map(k=>
      '<div class="kcard"><div class="kname">'+k.name+'</div><div class="kmeta">'+
      (k.direction?'<span>↕ '+k.direction+'</span>':'')+
      (k.benchmark?'<span>🎯 '+k.benchmark.slice(0,60)+'</span>':'')+
      '</div></div>').join('');
  } else {
    const up = UP[n.id]||[], dn = DN[n.id]||[];
    let h = '';
    if (up.length) {
      h += '<div class="sec" style="margin-top:0">Upstream (feeds into this)</div>';
      h += up.map(e => {
        const s = NM[e.id]||{};
        return '<div class="larrow" onclick="showDetail(\''+e.id+'\');network.selectNodes([\''+e.id+'\'])">'+
          '<span class="arr">←</span>'+
          '<div><div class="lname">'+e.id+'</div><div class="lplat">'+(s.platform||'')+' · '+(s.layer||'')+'</div></div>'+
          '<span class="ltype">'+e.type+'</span></div>';
      }).join('');
    }
    if (dn.length) {
      h += '<div class="sec">Downstream (this feeds into)</div>';
      h += dn.map(e => {
        const s = NM[e.id]||{};
        return '<div class="larrow" onclick="showDetail(\''+e.id+'\');network.selectNodes([\''+e.id+'\'])">'+
          '<span class="arr">→</span>'+
          '<div><div class="lname">'+e.id+'</div><div class="lplat">'+(s.platform||'')+' · '+(s.layer||'')+'</div></div>'+
          '<span class="ltype">'+e.type+'</span></div>';
      }).join('');
    }
    if (!up.length && !dn.length) h = '<p style="color:#8b949e;font-size:11px">No lineage edges registered.</p>';
    body.innerHTML = h;
  }
}

// ── init
applyFilters();
network.once('stabilized', fitAll);
</script>
</body>
</html>"""


def build_rich_html(data: dict) -> str:
    """Inject graph data into the HTML template via simple string replacement."""
    data_json = json.dumps(data, ensure_ascii=False)
    return _HTML_TEMPLATE.replace("__DATA__", data_json)
