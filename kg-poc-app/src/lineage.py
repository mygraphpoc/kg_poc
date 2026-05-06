"""src/lineage.py — Lineage graph data loading and pyvis HTML builder."""

import json
import os
import tempfile
import streamlit as st
from src import graphdb


@st.cache_data(show_spinner=False, ttl=300)
def load(_token: str) -> tuple[list, list]:
    """Return (nodes, edges) from GraphDB. Cached 5 min."""
    nodes = graphdb.query("""
        SELECT DISTINCT ?tname ?layer ?platform ?domain WHERE {
            ?t biz:tableName ?tname .
            OPTIONAL { ?t biz:tableLayer ?layer }
            OPTIONAL { ?t biz:sourceSystemType ?platform }
            OPTIONAL { ?t biz:tableDomain ?domain }
        }""", _token) or []

    edges = graphdb.query("""
        SELECT DISTINCT ?srcName ?tgtName ?transformType WHERE {
            ?src biz:feedsInto ?tgt .
            ?src biz:tableName ?srcName .
            ?tgt biz:tableName ?tgtName .
            ?edge biz:sourceTable ?src ; biz:targetTable ?tgt .
            OPTIONAL { ?edge biz:lineageTransformType ?transformType }
        }""", _token) or []

    return nodes, edges


def fetch_detail(_token: str, table_name: str) -> dict:
    tn = table_name.lower()
    cols = graphdb.query(f"""
        SELECT ?colName ?isPII ?isKPI WHERE {{
            ?t biz:tableName ?tname ; biz:hasColumn ?col .
            FILTER (LCASE(STR(?tname)) = "{tn}")
            ?col biz:columnName ?colName .
            OPTIONAL {{ ?col biz:isPII ?isPII }}
            OPTIONAL {{ ?col biz:isKPIColumn ?isKPI }}
        }} ORDER BY ?colName""", _token) or []

    kpis = graphdb.query(f"""
        SELECT ?kpiName ?direction ?benchmark WHERE {{
            ?t biz:tableName ?tname ; biz:hasKPI ?kpi .
            FILTER (LCASE(STR(?tname)) = "{tn}")
            ?kpi biz:kpiName ?kpiName .
            OPTIONAL {{ ?kpi biz:kpiDirection ?direction }}
            OPTIONAL {{ ?kpi biz:kpiBenchmark ?benchmark }}
        }}""", _token) or []

    upstream = graphdb.query(f"""
        SELECT ?srcName ?srcPlatform ?transformType WHERE {{
            ?tgt biz:tableName ?tname .
            FILTER (LCASE(STR(?tname)) = "{tn}")
            ?src biz:feedsInto ?tgt ; biz:tableName ?srcName .
            OPTIONAL {{ ?src biz:sourceSystemType ?srcPlatform }}
            ?edge biz:sourceTable ?src ; biz:targetTable ?tgt .
            OPTIONAL {{ ?edge biz:lineageTransformType ?transformType }}
        }}""", _token) or []

    downstream = graphdb.query(f"""
        SELECT ?tgtName ?tgtPlatform WHERE {{
            ?src biz:tableName ?tname .
            FILTER (LCASE(STR(?tname)) = "{tn}")
            ?src biz:feedsInto ?tgt .
            ?tgt biz:tableName ?tgtName .
            OPTIONAL {{ ?tgt biz:sourceSystemType ?tgtPlatform }}
        }}""", _token) or []

    return {"columns": cols, "kpis": kpis, "upstream": upstream, "downstream": downstream}


LAYER_STYLE = {
    "bronze":  {"color": "#CD853F", "border": "#8B4513", "shape": "triangleDown"},
    "silver":  {"color": "#607D8B", "border": "#37474F", "shape": "ellipse"},
    "gold":    {"color": "#F9A825", "border": "#E65100", "shape": "star"},
    "staging": {"color": "#7B1FA2", "border": "#4A148C", "shape": "square"},
}
PLATFORM_BORDER  = {"databricks": "#FF7043", "snowflake": "#29B5E8"}
EDGE_COLOR       = {"replicate":"#58a6ff","aggregate":"#f9a825","join":"#56d364","transform":"#f0883e"}


def build_graph(nodes: list, edges: list, layer_filter: list,
                platform_filter: list, domain_filter: list, search: str) -> str:
    """Return pyvis Network as an HTML string, filtered to the selections."""
    try:
        from pyvis.network import Network
    except ImportError:
        return "<p style='color:#f85149;padding:20px'>Install pyvis: pip install pyvis</p>"

    # Decide which table names are visible
    visible: set[str] = set()
    for n in nodes:
        tn  = n.get("tname","")
        lay = (n.get("layer")    or "").lower()
        plat= (n.get("platform") or "").lower()
        dom = (n.get("domain")   or "").lower()
        if layer_filter    and lay  not in layer_filter:    continue
        if platform_filter and plat not in platform_filter: continue
        if domain_filter   and dom  not in [d.lower() for d in domain_filter]: continue
        if search and search.lower() not in tn.lower():     continue
        visible.add(tn)

    net = Network(height="640px", width="100%",
                  bgcolor="#0d1117", font_color="#c9d1d9", directed=True)
    net.barnes_hut(spring_length=180, spring_strength=0.04, damping=0.09)

    # Degree (for node sizing)
    degree: dict[str,int] = {}
    for e in edges:
        for k in ("srcName","tgtName"):
            degree[e.get(k,"")] = degree.get(e.get(k,""), 0) + 1

    node_meta = {n.get("tname",""): n for n in nodes}
    added: set[str] = set()

    for tn in visible:
        m    = node_meta.get(tn, {})
        lay  = (m.get("layer")    or "unknown").lower()
        plat = (m.get("platform") or "").lower()
        dom  = (m.get("domain")   or "").capitalize()
        sty  = LAYER_STYLE.get(lay, {"color":"#58a6ff","border":"#1f6feb","shape":"ellipse"})
        size = 18 + min(degree.get(tn, 0) * 3, 30)
        tip  = (f"<b>{tn}</b><br/>Layer: {lay}<br/>Platform: {plat}"
                f"<br/>Domain: {dom}<br/>Connections: {degree.get(tn,0)}")
        net.add_node(tn, label=tn, title=tip,
                     color={"background": sty["color"],
                            "border": PLATFORM_BORDER.get(plat, sty["border"]),
                            "highlight": {"background":"#ffffff"}},
                     shape=sty["shape"], size=size,
                     font={"size":11,"color":"#e6edf3","face":"monospace"})
        added.add(tn)

    for e in edges:
        src = e.get("srcName",""); tgt = e.get("tgtName","")
        if src not in added or tgt not in added: continue
        tt  = (e.get("transformType") or "lineage").lower()
        net.add_edge(src, tgt, color=EDGE_COLOR.get(tt,"#444d56"),
                     title=tt, arrows="to", width=1.5)

    net.set_options(json.dumps({
        "physics": {"enabled":True,
                    "barnesHut":{"springLength":180,"springConstant":0.04,"damping":0.09}},
        "edges":   {"smooth":{"type":"curvedCW","roundness":0.2}},
        "interaction": {"hover":True,"tooltipDelay":150},
    }))

    with tempfile.NamedTemporaryFile(suffix=".html", delete=False,
                                     mode="w", encoding="utf-8") as f:
        net.save_graph(f.name)
        fname = f.name
    with open(fname, encoding="utf-8") as f:
        html = f.read()
    os.unlink(fname)
    return html
