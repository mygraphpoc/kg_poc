"""src/lineage.py — Lineage data loading + rich vis.js graph builder."""

import json
import streamlit as st
from src import graphdb


# ─── Data loading ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=300)
def load_full(_token: str) -> dict:
    """
    One-shot load of ALL graph data needed to render the rich lineage UI.
    Returns a dict ready to be JSON-serialised and injected into the HTML.
    """
    # Nodes
    node_rows = graphdb.query("""
        SELECT DISTINCT ?tname ?layer ?platform ?domain WHERE {
            ?t biz:tableName ?tname .
            OPTIONAL { ?t biz:tableLayer ?layer }
            OPTIONAL { ?t biz:sourceSystemType ?platform }
            OPTIONAL { ?t biz:tableDomain ?domain }
        }""", _token) or []

    # Edges
    edge_rows = graphdb.query("""
        SELECT DISTINCT ?srcName ?tgtName ?transformType WHERE {
            ?src biz:feedsInto ?tgt .
            ?src biz:tableName ?srcName .
            ?tgt biz:tableName ?tgtName .
            ?edge biz:sourceTable ?src ; biz:targetTable ?tgt .
            OPTIONAL { ?edge biz:lineageTransformType ?transformType }
        }""", _token) or []

    # Columns (all tables at once)
    col_rows = graphdb.query("""
        SELECT ?tname ?colName ?isPII ?isKPI WHERE {
            ?t biz:tableName ?tname ; biz:hasColumn ?col .
            ?col biz:columnName ?colName .
            OPTIONAL { ?col biz:isPII ?isPII }
            OPTIONAL { ?col biz:isKPIColumn ?isKPI }
        } ORDER BY ?tname ?colName""", _token) or []

    # KPIs (all tables at once)
    kpi_rows = graphdb.query("""
        SELECT ?tname ?kpiName ?direction ?benchmark WHERE {
            ?t biz:tableName ?tname ; biz:hasKPI ?kpi .
            ?kpi biz:kpiName ?kpiName .
            OPTIONAL { ?kpi biz:kpiDirection ?direction }
            OPTIONAL { ?kpi biz:kpiBenchmark ?benchmark }
        } ORDER BY ?tname ?kpiName""", _token) or []

    # Group columns and KPIs by table
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

    # Compute degree
    degree: dict = {}
    for e in edge_rows:
        for k in ("srcName", "tgtName"):
            n = e.get(k, "")
            degree[n] = degree.get(n, 0) + 1

    # Build node list
    nodes = []
    for r in node_rows:
        tn = r.get("tname", "")
        if not tn:
            continue
        layer    = (r.get("layer")    or "").lower()
        platform = (r.get("platform") or "").lower()
        domain   = (r.get("domain")   or "").capitalize()
        nodes.append({
            "id":       tn,
            "label":    tn,
            "layer":    layer,
            "platform": platform,
            "domain":   domain,
            "degree":   degree.get(tn, 0),
            "columns":  cols_by_table.get(tn, []),
            "kpis":     kpis_by_table.get(tn, []),
        })

    edges = []
    for r in edge_rows:
        src = r.get("srcName", "")
        tgt = r.get("tgtName", "")
        if src and tgt:
            edges.append({
                "from": src,
                "to":   tgt,
                "type": (r.get("transformType") or "lineage").lower(),
            })

    # Collect filter options
    layers    = sorted({n["layer"]    for n in nodes if n["layer"]})
    platforms = sorted({n["platform"] for n in nodes if n["platform"]})
    domains   = sorted({n["domain"]   for n in nodes if n["domain"]})

    return {
        "nodes":     nodes,
        "edges":     edges,
        "layers":    layers,
        "platforms": platforms,
        "domains":   domains,
    }


# ─── HTML builder ──────────────────────────────────────────────────────────────

def build_rich_html(data: dict) -> str:
    """Return a self-contained HTML page with a dynamic vis.js lineage graph."""
    data_json = json.dumps(data, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Data Lineage</title>
<script src="https://cdn.jsdelivr.net/npm/vis-network@9.1.9/standalone/umd/vis-network.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0d1117;color:#c9d1d9;font-family:'Segoe UI',system-ui,sans-serif;font-size:13px;height:100vh;overflow:hidden}}

  /* ── Layout ── */
  #shell{{display:grid;grid-template-columns:240px 1fr 310px;grid-template-rows:50px 1fr;height:100vh}}
  #topbar{{grid-column:1/-1;background:#161b22;border-bottom:1px solid #30363d;
           display:flex;align-items:center;gap:10px;padding:0 16px}}
  #left{{background:#161b22;border-right:1px solid #30363d;overflow-y:auto;padding:12px}}
  #center{{position:relative;overflow:hidden}}
  #right{{background:#161b22;border-left:1px solid #30363d;overflow-y:auto}}
  #network{{width:100%;height:100%}}

  /* ── Topbar ── */
  #search{{background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#c9d1d9;
           padding:5px 10px;width:240px;outline:none;font-size:12px}}
  #search:focus{{border-color:#58a6ff}}
  .tbtn{{background:#21262d;border:1px solid #30363d;border-radius:6px;color:#c9d1d9;
          cursor:pointer;padding:4px 10px;font-size:12px;white-space:nowrap}}
  .tbtn:hover{{border-color:#58a6ff;color:#58a6ff}}
  .tbtn.active{{background:#1f6feb;border-color:#58a6ff;color:#fff}}
  #stats-bar{{margin-left:auto;font-size:11px;color:#8b949e;white-space:nowrap}}

  /* ── Left panel ── */
  .section-title{{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;
                  color:#8b949e;margin:14px 0 6px}}
  .filter-group{{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px}}
  .chip{{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:12px;
          cursor:pointer;font-size:11px;border:1px solid transparent;transition:.15s}}
  .chip:hover{{opacity:.85}}

  /* layer chips */
  .chip-bronze{{background:#2d1f0e;color:#e8a55a;border-color:#8B4513}}
  .chip-silver{{background:#141f2a;color:#79afd1;border-color:#37474F}}
  .chip-gold  {{background:#2a2100;color:#f9c846;border-color:#b8860b}}
  .chip-staging{{background:#1e1228;color:#bc8cff;border-color:#4A148C}}
  /* platform chips */
  .chip-databricks{{background:#2a1800;color:#ff8c42;border-color:#d1571a}}
  .chip-snowflake {{background:#0d1e2d;color:#5ab5e8;border-color:#1f6feb}}
  .chip.off{{opacity:.35;filter:grayscale(1)}}

  .legend-item{{display:flex;align-items:center;gap:7px;margin-bottom:6px;font-size:11px;color:#8b949e}}
  .legend-dot{{width:12px;height:12px;border-radius:2px;flex-shrink:0}}
  .stat-row{{display:flex;justify-content:space-between;margin-bottom:5px;font-size:11px}}
  .stat-val{{color:#58a6ff;font-weight:600}}

  /* ── Detail panel ── */
  #detail-header{{padding:16px 14px 10px;border-bottom:1px solid #21262d}}
  #detail-name{{font-size:14px;font-weight:600;color:#e6edf3;word-break:break-all;margin-bottom:8px}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;margin-right:4px;margin-bottom:4px}}
  .badge-gold     {{background:#2a2100;color:#f9c846;border:1px solid #b8860b}}
  .badge-silver   {{background:#141f2a;color:#79afd1;border:1px solid #37474F}}
  .badge-bronze   {{background:#2d1f0e;color:#e8a55a;border:1px solid #8B4513}}
  .badge-staging  {{background:#1e1228;color:#bc8cff;border:1px solid #4A148C}}
  .badge-databricks{{background:#2a1800;color:#ff8c42;border:1px solid #d1571a}}
  .badge-snowflake {{background:#0d1e2d;color:#5ab5e8;border:1px solid #1f6feb}}
  .badge-domain   {{background:#21262d;color:#8b949e;border:1px solid #30363d}}

  #detail-tabs{{display:flex;border-bottom:1px solid #21262d}}
  .dtab{{padding:8px 14px;cursor:pointer;font-size:12px;color:#8b949e;
          border-bottom:2px solid transparent;transition:.15s}}
  .dtab:hover{{color:#c9d1d9}}
  .dtab.active{{color:#58a6ff;border-bottom-color:#58a6ff}}
  #detail-body{{padding:14px}}

  .col-table{{width:100%;border-collapse:collapse;font-size:11px}}
  .col-table th{{color:#8b949e;text-align:left;padding:4px 6px;
                  border-bottom:1px solid #21262d;font-weight:600;text-transform:uppercase;
                  font-size:10px;letter-spacing:.05em}}
  .col-table td{{padding:4px 6px;border-bottom:1px solid #161b22;color:#c9d1d9}}
  .col-table tr:hover td{{background:#21262d}}
  .flag{{font-size:10px;padding:1px 5px;border-radius:8px;margin-left:3px}}
  .flag-pii{{background:#5a0a0a;color:#ffa198}}
  .flag-kpi{{background:#0a3a1a;color:#56d364}}

  .kpi-card{{background:#0d1117;border:1px solid #21262d;border-radius:6px;
              padding:10px;margin-bottom:8px}}
  .kpi-name{{font-size:12px;font-weight:600;color:#e6edf3;margin-bottom:4px}}
  .kpi-meta{{font-size:11px;color:#8b949e;display:flex;gap:12px;flex-wrap:wrap}}

  .lineage-arrow{{display:flex;align-items:center;padding:7px 0;
                   border-bottom:1px solid #21262d;gap:8px;cursor:pointer}}
  .lineage-arrow:hover .lineage-name{{color:#58a6ff}}
  .lineage-name{{font-size:12px;font-weight:500;color:#c9d1d9}}
  .lineage-plat{{font-size:10px;color:#8b949e}}
  .lineage-type{{font-size:10px;background:#21262d;padding:2px 6px;border-radius:8px;color:#8b949e}}
  .arr{{font-size:16px;color:#30363d;flex-shrink:0}}

  #placeholder{{display:flex;flex-direction:column;align-items:center;justify-content:center;
                height:100%;color:#8b949e;text-align:center;padding:30px;gap:12px}}
  #placeholder .icon{{font-size:36px}}
  #placeholder .hint{{font-size:12px;line-height:1.6}}

  /* mini stats chips at top of detail */
  #mini-stats{{display:flex;gap:8px;padding:10px 14px;border-bottom:1px solid #21262d;flex-wrap:wrap}}
  .mini-stat{{background:#0d1117;border:1px solid #21262d;border-radius:6px;
               padding:6px 10px;text-align:center;flex:1;min-width:60px}}
  .mini-stat .n{{font-size:15px;font-weight:700;color:#58a6ff}}
  .mini-stat .l{{font-size:9px;color:#8b949e;text-transform:uppercase;letter-spacing:.05em}}

  /* controls in center */
  #center-controls{{position:absolute;top:10px;right:10px;display:flex;flex-direction:column;gap:6px;z-index:10}}
  .icon-btn{{background:#161b22;border:1px solid #30363d;border-radius:6px;
              color:#8b949e;cursor:pointer;padding:6px 8px;font-size:13px;line-height:1}}
  .icon-btn:hover{{border-color:#58a6ff;color:#58a6ff}}
</style>
</head>
<body>
<div id="shell">

  <!-- Top bar -->
  <div id="topbar">
    <input id="search" placeholder="🔍 Search tables…" oninput="applyFilters()"/>
    <button class="tbtn active" id="btn-gold"    onclick="toggleLayer('gold')"   >⭐ Gold</button>
    <button class="tbtn active" id="btn-silver"  onclick="toggleLayer('silver')" >● Silver</button>
    <button class="tbtn active" id="btn-bronze"  onclick="toggleLayer('bronze')" >▼ Bronze</button>
    <button class="tbtn active" id="btn-staging" onclick="toggleLayer('staging')">■ Staging</button>
    <span style="width:1px;background:#30363d;height:20px;margin:0 4px"></span>
    <button class="tbtn active" id="btn-databricks" onclick="togglePlatform('databricks')">🟠 Databricks</button>
    <button class="tbtn active" id="btn-snowflake"  onclick="togglePlatform('snowflake')">❄️ Snowflake</button>
    <button class="tbtn" onclick="fitAll()" style="margin-left:6px">⊞ Fit</button>
    <button class="tbtn" onclick="resetFilters()">↺ Reset</button>
    <span id="stats-bar">Loading…</span>
  </div>

  <!-- Left: legend + domain filters + stats -->
  <div id="left">
    <div class="section-title">Node type</div>
    <div class="legend-item"><div class="legend-dot" style="background:#F9A825;border-radius:50%;"></div> Gold / Aggregate</div>
    <div class="legend-item"><div class="legend-dot" style="background:#78909C;border:2px solid #546E7A"></div> Silver / Dimension</div>
    <div class="legend-item"><div class="legend-dot" style="background:#607D8B;transform:rotate(45deg)"></div> Silver / Fact</div>
    <div class="legend-item"><div class="legend-dot" style="background:#CD853F;clip-path:polygon(50% 0%,0% 100%,100% 100%)"></div> Bronze / Raw</div>
    <div class="legend-item"><div class="legend-dot" style="background:#7B1FA2;border-radius:2px"></div> Staging</div>

    <div class="section-title">Platform</div>
    <div class="legend-item"><span style="color:#FF7043">●</span>&nbsp;Databricks border</div>
    <div class="legend-item"><span style="color:#29B5E8">●</span>&nbsp;Snowflake border</div>

    <div class="section-title">Domain</div>
    <div class="filter-group" id="domain-chips"></div>

    <div class="section-title">Stats</div>
    <div id="left-stats"></div>
  </div>

  <!-- Center: graph + floating controls -->
  <div id="center">
    <div id="network"></div>
    <div id="center-controls">
      <button class="icon-btn" onclick="network.zoomIn()" title="Zoom in">＋</button>
      <button class="icon-btn" onclick="network.zoomOut()" title="Zoom out">－</button>
      <button class="icon-btn" onclick="fitAll()" title="Fit all">⊞</button>
      <button class="icon-btn" onclick="togglePhysics()" id="physBtn" title="Toggle physics">⚛</button>
    </div>
  </div>

  <!-- Right: node detail -->
  <div id="right">
    <div id="placeholder">
      <div class="icon">🔷</div>
      <div class="hint">Click any table in the graph<br/>to explore its metadata,<br/>columns, KPIs and lineage.</div>
    </div>
    <div id="detail-content" style="display:none">
      <div id="detail-header">
        <div id="detail-name"></div>
        <div id="detail-badges"></div>
      </div>
      <div id="mini-stats"></div>
      <div id="detail-tabs">
        <div class="dtab active" onclick="switchTab('overview')">Overview</div>
        <div class="dtab" onclick="switchTab('columns')">Columns</div>
        <div class="dtab" onclick="switchTab('kpis')">KPIs</div>
        <div class="dtab" onclick="switchTab('lineage')">Lineage</div>
      </div>
      <div id="detail-body"></div>
    </div>
  </div>

</div>

<script>
// ── Data injected from Python ──────────────────────────────────────────────
const RAW = {data_json};

// ── State ─────────────────────────────────────────────────────────────────
const activeLayers    = new Set(RAW.layers);
const activePlatforms = new Set(RAW.platforms);
const activeDomains   = new Set(RAW.domains);
let   selectedNode    = null;
let   physicsOn       = true;
let   currentTab      = 'overview';

// ── Build lookup ───────────────────────────────────────────────────────────
const nodeMap = {{}};
RAW.nodes.forEach(n => nodeMap[n.id] = n);

// ── Neighbours lookup ──────────────────────────────────────────────────────
const upstream   = {{}};  // nodeId -> [srcId, ...]
const downstream = {{}};  // nodeId -> [tgtId, ...]
RAW.edges.forEach(e => {{
  upstream[e.to]     = upstream[e.to]     || [];
  downstream[e.from] = downstream[e.from] || [];
  upstream[e.to].push({{id:e.from, type:e.type}});
  downstream[e.from].push({{id:e.to,   type:e.type}});
}});

// ── Node visual ───────────────────────────────────────────────────────────
const LAYER_COLOR = {{
  gold:    {{bg:'#F9A825', border:'#E65100'}},
  silver:  {{bg:'#607D8B', border:'#37474F'}},
  bronze:  {{bg:'#A0522D', border:'#6B3410'}},
  staging: {{bg:'#7B1FA2', border:'#4A148C'}},
}};
const PLAT_BORDER = {{databricks:'#FF7043', snowflake:'#29B5E8'}};

function nodeShape(n) {{
  const l = n.layer;
  const label = n.id.toLowerCase();
  if (l === 'gold')    return 'star';
  if (l === 'staging') return 'square';
  if (l === 'bronze')  return 'triangleDown';
  // silver: fact = diamond, dim = ellipse
  if (label.startsWith('fct_') || label.startsWith('fact_')) return 'diamond';
  return 'ellipse';
}}

function nodeSize(n) {{
  const base = n.layer === 'gold' ? 20 : n.layer === 'silver' ? 16 : 12;
  return base + Math.min(n.degree * 3, 20);
}}

function visNode(n) {{
  const lc = LAYER_COLOR[n.layer] || {{bg:'#58a6ff', border:'#1f6feb'}};
  const border = PLAT_BORDER[n.platform] || lc.border;
  return {{
    id:    n.id,
    label: n.id.length > 22 ? n.id.slice(0,20)+'…' : n.id,
    title: `<b>${{n.id}}</b><br/>Layer: ${{n.layer}} | Platform: ${{n.platform}}<br/>Domain: ${{n.domain}}<br/>Cols: ${{n.columns.length}} | KPIs: ${{n.kpis.length}}`,
    shape: nodeShape(n),
    size:  nodeSize(n),
    color: {{ background:lc.bg, border:border,
              highlight:{{background:'#ffffff', border:border}},
              hover:{{background:'#e6edf3', border:border}} }},
    font:  {{size:11, color:'#e6edf3', face:'Segoe UI'}},
  }};
}}

function visEdge(e) {{
  const colors = {{replicate:'#58a6ff',aggregate:'#f9c846',join:'#56d364',transform:'#f0883e',lineage:'#444d56'}};
  return {{
    from:   e.from, to: e.to,
    color:  {{color: colors[e.type]||'#444d56', hover:'#8b949e', highlight:'#ffffff'}},
    width:  1.5,
    arrows: {{to:{{enabled:true,scaleFactor:.6}}}},
    smooth: {{type:'curvedCW', roundness:.15}},
    title:  e.type,
  }};
}}

// ── vis.js DataSets + Network ─────────────────────────────────────────────
const nodesDS = new vis.DataSet(RAW.nodes.map(visNode));
const edgesDS = new vis.DataSet(RAW.edges.map(visEdge));

const network = new vis.Network(
  document.getElementById('network'),
  {{nodes: nodesDS, edges: edgesDS}},
  {{
    physics: {{
      enabled: true,
      barnesHut: {{springLength:180, springConstant:.04, damping:.09, gravitationalConstant:-3000}},
    }},
    interaction: {{hover:true, tooltipDelay:200, multiselect:false}},
    layout: {{improvedLayout:true, clusterThreshold:150}},
  }}
);

// ── Filters ────────────────────────────────────────────────────────────────
function visibleIds() {{
  const q = document.getElementById('search').value.toLowerCase();
  return RAW.nodes
    .filter(n =>
      (activeLayers.has(n.layer) || !n.layer) &&
      (activePlatforms.has(n.platform) || !n.platform) &&
      (activeDomains.size === 0 || activeDomains.has(n.domain)) &&
      (!q || n.id.toLowerCase().includes(q))
    ).map(n => n.id);
}}

function applyFilters() {{
  const ids = new Set(visibleIds());
  RAW.nodes.forEach(n => {{
    const hidden = !ids.has(n.id);
    nodesDS.update({{id:n.id, hidden}});
  }});
  RAW.edges.forEach(e => {{
    const hidden = !ids.has(e.from) || !ids.has(e.to);
    edgesDS.update({{from:e.from, to:e.to, hidden}});
  }});
  updateStats();
}}

function updateStats() {{
  const visible = RAW.nodes.filter(n => !nodesDS.get(n.id).hidden);
  const vIds = new Set(visible.map(n=>n.id));
  const visEdges = RAW.edges.filter(e=>vIds.has(e.from)&&vIds.has(e.to));
  document.getElementById('stats-bar').textContent =
    `${{visible.length}} tables · ${{visEdges.length}} edges`;

  const ls = {{}};
  visible.forEach(n=>{{ls[n.layer]=(ls[n.layer]||0)+1}});
  document.getElementById('left-stats').innerHTML =
    Object.entries(ls).map(([l,c])=>
      `<div class="stat-row"><span style="text-transform:capitalize">${{l}}</span><span class="stat-val">${{c}}</span></div>`
    ).join('');
}}

function toggleLayer(l) {{
  if(activeLayers.has(l)) activeLayers.delete(l); else activeLayers.add(l);
  document.getElementById('btn-'+l).classList.toggle('active', activeLayers.has(l));
  applyFilters();
}}
function togglePlatform(p) {{
  if(activePlatforms.has(p)) activePlatforms.delete(p); else activePlatforms.add(p);
  document.getElementById('btn-'+p).classList.toggle('active', activePlatforms.has(p));
  applyFilters();
}}
function toggleDomain(d, el) {{
  if(activeDomains.has(d)) activeDomains.delete(d); else activeDomains.add(d);
  el.classList.toggle('off', !activeDomains.has(d));
  applyFilters();
}}
function resetFilters() {{
  RAW.layers.forEach(l=>activeLayers.add(l));
  RAW.platforms.forEach(p=>activePlatforms.add(p));
  RAW.domains.forEach(d=>activeDomains.add(d));
  document.getElementById('search').value='';
  document.querySelectorAll('.tbtn').forEach(b=>b.classList.add('active'));
  document.querySelectorAll('#domain-chips .chip').forEach(c=>c.classList.remove('off'));
  applyFilters();
  fitAll();
}}
function fitAll() {{ network.fit({{animation:{{duration:400,easingFunction:'easeInOutQuad'}}}}) }}
function togglePhysics() {{
  physicsOn = !physicsOn;
  network.setOptions({{physics:{{enabled:physicsOn}}}});
  document.getElementById('physBtn').style.color = physicsOn ? '#56d364' : '#8b949e';
}}

// ── Domain chips ──────────────────────────────────────────────────────────
const chipContainer = document.getElementById('domain-chips');
RAW.domains.forEach(d => {{
  const c = document.createElement('span');
  c.className = 'chip chip-domain';
  c.style.cssText='background:#21262d;color:#8b949e;border-color:#30363d';
  c.textContent = d;
  c.onclick = () => toggleDomain(d, c);
  chipContainer.appendChild(c);
}});

// ── Click handler ─────────────────────────────────────────────────────────
network.on('click', params => {{
  if (params.nodes.length > 0) {{
    const id = params.nodes[0];
    selectNode(id);
    // Highlight connected nodes
    const connected = network.getConnectedNodes(id);
    network.selectNodes([id,...connected]);
  }} else {{
    clearDetail();
    network.unselectAll();
  }}
}});

network.on('doubleClick', params => {{
  if (params.nodes.length > 0) fitAll();
}});

// ── Detail panel ──────────────────────────────────────────────────────────
function selectNode(id) {{
  selectedNode = id;
  const n = nodeMap[id];
  if (!n) return;

  document.getElementById('placeholder').style.display='none';
  document.getElementById('detail-content').style.display='block';

  document.getElementById('detail-name').textContent = n.id;

  const badges = [
    `<span class="badge badge-${{n.layer}}">${{n.layer.toUpperCase()}}</span>`,
    n.platform ? `<span class="badge badge-${{n.platform}}">${{n.platform === 'databricks' ? '🟠' : '❄️'}} ${{n.platform}}</span>` : '',
    n.domain   ? `<span class="badge badge-domain">${{n.domain}}</span>` : '',
  ].join('');
  document.getElementById('detail-badges').innerHTML = badges;

  const up = (upstream[id]||[]).length;
  const dn = (downstream[id]||[]).length;
  document.getElementById('mini-stats').innerHTML = `
    <div class="mini-stat"><div class="n">${{n.columns.length}}</div><div class="l">Columns</div></div>
    <div class="mini-stat"><div class="n">${{n.kpis.length}}</div><div class="l">KPIs</div></div>
    <div class="mini-stat"><div class="n">${{up}}</div><div class="l">Upstream</div></div>
    <div class="mini-stat"><div class="n">${{dn}}</div><div class="l">Downstream</div></div>
  `;

  switchTab(currentTab);
}}

function switchTab(tab) {{
  currentTab = tab;
  document.querySelectorAll('.dtab').forEach((el,i) => {{
    el.classList.toggle('active', ['overview','columns','kpis','lineage'][i] === tab);
  }});
  if (!selectedNode) return;
  const n = nodeMap[selectedNode];
  const body = document.getElementById('detail-body');

  if (tab === 'overview') {{
    body.innerHTML = `
      <p style="color:#8b949e;font-size:11px;margin-bottom:12px">
        ${{n.layer === 'gold' ? '⭐ Gold KPI aggregate table' :
           n.layer === 'silver' ? '● Silver fact/dimension table' :
           n.layer === 'bronze' ? '▼ Raw bronze table' : '■ Staging table'}}
      </p>
      <div class="stat-row"><span>Columns</span><span class="stat-val">${{n.columns.length}}</span></div>
      <div class="stat-row"><span>KPIs registered</span><span class="stat-val">${{n.kpis.length}}</span></div>
      <div class="stat-row"><span>Upstream tables</span><span class="stat-val">${{(upstream[n.id]||[]).length}}</span></div>
      <div class="stat-row"><span>Downstream tables</span><span class="stat-val">${{(downstream[n.id]||[]).length}}</span></div>
      <div class="stat-row"><span>Total connections</span><span class="stat-val">${{n.degree}}</span></div>
      <div class="stat-row"><span>Platform</span><span class="stat-val">${{n.platform||'—'}}</span></div>
      <div class="stat-row"><span>Domain</span><span class="stat-val">${{n.domain||'—'}}</span></div>
    `;
  }}

  else if (tab === 'columns') {{
    if (!n.columns.length) {{
      body.innerHTML = '<p style="color:#8b949e;font-size:11px;padding-top:4px">No column metadata in graph.</p>';
      return;
    }}
    const rows = n.columns.map(c =>
      `<tr>
        <td>${{c.name}}${{c.pii?'<span class="flag flag-pii">PII</span>':''}}${{c.kpi?'<span class="flag flag-kpi">KPI</span>':''}}</td>
      </tr>`
    ).join('');
    body.innerHTML = `
      <table class="col-table">
        <thead><tr><th>Column name</th></tr></thead>
        <tbody>${{rows}}</tbody>
      </table>`;
  }}

  else if (tab === 'kpis') {{
    if (!n.kpis.length) {{
      body.innerHTML = '<p style="color:#8b949e;font-size:11px;padding-top:4px">No KPIs registered for this table.</p>';
      return;
    }}
    body.innerHTML = n.kpis.map(k => `
      <div class="kpi-card">
        <div class="kpi-name">${{k.name}}</div>
        <div class="kpi-meta">
          ${{k.direction ? `<span>↕ ${{k.direction}}</span>` : ''}}
          ${{k.benchmark ? `<span>🎯 ${{k.benchmark.slice(0,60)}}</span>` : ''}}
        </div>
      </div>`).join('');
  }}

  else if (tab === 'lineage') {{
    const up   = (upstream[n.id]||[]);
    const dn   = (downstream[n.id]||[]);
    let html = '';
    if (up.length) {{
      html += `<div class="section-title" style="margin-top:0">Upstream — feeds into this table</div>`;
      html += up.map(e => {{
        const src = nodeMap[e.id]||{{}};
        return `<div class="lineage-arrow" onclick="selectNode('${{e.id}}');network.selectNodes(['${{e.id}}'])">
          <span class="arr">←</span>
          <div><div class="lineage-name">${{e.id}}</div>
               <div class="lineage-plat">${{src.platform||''}} · ${{src.layer||''}}</div></div>
          <span class="lineage-type" style="margin-left:auto">${{e.type}}</span>
        </div>`;
      }}).join('');
    }}
    if (dn.length) {{
      html += `<div class="section-title">Downstream — this feeds into</div>`;
      html += dn.map(e => {{
        const tgt = nodeMap[e.id]||{{}};
        return `<div class="lineage-arrow" onclick="selectNode('${{e.id}}');network.selectNodes(['${{e.id}}'])">
          <span class="arr">→</span>
          <div><div class="lineage-name">${{e.id}}</div>
               <div class="lineage-plat">${{tgt.platform||''}} · ${{tgt.layer||''}}</div></div>
          <span class="lineage-type" style="margin-left:auto">${{e.type}}</span>
        </div>`;
      }}).join('');
    }}
    if (!up.length && !dn.length) html = '<p style="color:#8b949e;font-size:11px">No lineage edges registered.</p>';
    body.innerHTML = html;
  }}
}}

function clearDetail() {{
  selectedNode = null;
  document.getElementById('placeholder').style.display='flex';
  document.getElementById('detail-content').style.display='none';
}}

// ── Init ─────────────────────────────────────────────────────────────────
applyFilters();
network.once('stabilized', fitAll);
</script>
</body>
</html>"""
