"""Gera REVERSA_DATALOGGERS.html replicando fielmente o dasboard_reversa_loggers.py"""
from __future__ import annotations
from pathlib import Path
import unicodedata
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.io as pio

import json

WORKSPACE    = Path(__file__).resolve().parents[1]
SNAPSHOT_DIR = WORKSPACE / "snapshot_reversa"
OUTPUT_FILE  = Path(__file__).resolve().parent / "REVERSA_DATALOGGERS.html"
PERIODOS     = [7, 30, 60]
PERIODO_PAD  = 30   # período padrão ao abrir


# ── helpers (idênticos ao dashboard) ─────────────────────────────────────────

def normalize_key(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()

def normalize_text_value(value) -> str:
    s = str(value) if value is not None else ""
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return s.strip().upper()

def first_non_blank(series: pd.Series) -> str:
    vals = series.dropna().astype(str).str.strip()
    vals = vals[vals != ""]
    return vals.iloc[0] if not vals.empty else ""

def fmt_int(n) -> str:
    return f"{int(n):,}".replace(",", ".")

def fmt_pct(v: float) -> str:
    return f"{v:.1f}%".replace(".", ",")

def safe_pct(num: int, den: int) -> float:
    return 0.0 if den <= 0 else (num / den) * 100.0

def _fig_div(fig) -> str:
    return pio.to_html(fig, include_plotlyjs=False, full_html=False,
                       config={"displayModeBar": False})


# ── build_model (idêntico ao dashboard) ──────────────────────────────────────

def build_model(base_loggers, base_agentes, recebimento, base_destinatarios) -> pd.DataFrame:
    for col in ["nr_pedido", "cd_referencia"]:
        base_loggers[col] = normalize_key(base_loggers[col])
    base_agentes["PEDIDO"]         = normalize_key(base_agentes["PEDIDO"])
    base_destinatarios["PEDIDO"]   = normalize_key(base_destinatarios["PEDIDO"])
    recebimento["ds_tag"]          = normalize_key(recebimento["ds_tag"])

    # filtro recebimento: apenas ações "receb/restaur" + destino EM ESTOQUE + finalidade SALDO ESTOQUE
    if "ds_acaomovimentacao" in recebimento.columns:
        acao = recebimento["ds_acaomovimentacao"].fillna("").astype(str).str.lower()
        recebimento = recebimento[acao.str.contains("receb", na=False) | acao.str.contains("restaur", na=False)].copy()
    if "ds_destino" in recebimento.columns:
        recebimento = recebimento[recebimento["ds_destino"].map(normalize_text_value).eq("EM ESTOQUE")].copy()
    if "ds_finalidade" in recebimento.columns:
        recebimento = recebimento[
            recebimento["ds_finalidade"].map(normalize_text_value).isin(["SALDO ESTOQUE", "SALDO DE ESTOQUE"])
        ].copy()

    base_loggers["dt_embalagem"]   = pd.to_datetime(base_loggers["dt_embalagem"], errors="coerce")
    base_agentes["DATA_ENTREGA"]   = pd.to_datetime(base_agentes["DATA_ENTREGA"], errors="coerce")
    recebimento["dt_historico"]    = pd.to_datetime(recebimento["dt_historico"], errors="coerce")

    agentes_agg = base_agentes.groupby("PEDIDO", as_index=False).agg({
        "AGENTE": first_non_blank, "UF Destinatario": first_non_blank, "DATA_ENTREGA": "min"
    })
    dest_agg = base_destinatarios.groupby("PEDIDO", as_index=False).agg({
        "CIDADE_DESTINO": first_non_blank, "UF_DESTINO": first_non_blank, "DESTINATARIO": first_non_blank
    })
    ultimo_hist = (recebimento.groupby("ds_tag", as_index=False)["dt_historico"]
                   .max().rename(columns={"dt_historico": "Ultimo_Historico"}))

    df = base_loggers.merge(agentes_agg, how="left", left_on="nr_pedido", right_on="PEDIDO")
    df = df.merge(dest_agg, how="left", left_on="nr_pedido", right_on="PEDIDO", suffixes=("", "_dest"))
    df = df.merge(ultimo_hist, how="left", left_on="cd_referencia", right_on="ds_tag")

    df["Status Retorno"] = "Pendente de Retorno"
    mask = (df["Ultimo_Historico"].notna() & df["DATA_ENTREGA"].notna()
            & (df["Ultimo_Historico"] > df["DATA_ENTREGA"]))
    df.loc[mask, "Status Retorno"] = "Retornado"

    df = df.rename(columns={
        "nr_pedido": "Pedido", "cd_referencia": "Logger",
        "UF Destinatario": "UF", "DATA_ENTREGA": "Data de Entrega", "AGENTE": "Agente",
        "UF_DESTINO": "UF Destino", "CIDADE_DESTINO": "Cidade Destino", "DESTINATARIO": "Destinatario",
    })
    df["_data_entrega_dt"] = pd.to_datetime(df["Data de Entrega"], errors="coerce")
    if "dt_embalagem" in df.columns:
        df["dt_embalagem"] = pd.to_datetime(df["dt_embalagem"], errors="coerce")
        df["_data_entrega_dt"] = df["_data_entrega_dt"].fillna(df["dt_embalagem"])
    return df


def _prefilter(base_loggers, base_agentes, recebimento, base_destinatarios, days):
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=max(days, 7))
    bl = base_loggers.copy()
    bl["dt_embalagem"] = pd.to_datetime(bl["dt_embalagem"], errors="coerce")
    bl = bl[bl["dt_embalagem"] >= cutoff]
    if bl.empty:
        return bl, base_agentes.head(0), recebimento.head(0), base_destinatarios.head(0)
    pedidos_set = set(normalize_key(bl["nr_pedido"]).loc[lambda s: s != ""])
    tags_set    = set(normalize_key(bl["cd_referencia"]).loc[lambda s: s != ""])
    ba = base_agentes.copy()
    ba["_k"] = normalize_key(ba["PEDIDO"])
    ba = ba[ba["_k"].isin(pedidos_set)].drop(columns=["_k"])
    bd = base_destinatarios.copy()
    bd["_k"] = normalize_key(bd["PEDIDO"])
    bd = bd[bd["_k"].isin(pedidos_set)].drop(columns=["_k"])
    rc = recebimento.copy()
    rc["_k"] = normalize_key(rc["ds_tag"])
    rc["dt_historico"] = pd.to_datetime(rc["dt_historico"], errors="coerce")
    rc = rc[(rc["_k"].isin(tags_set)) & (rc["dt_historico"] >= cutoff - pd.Timedelta(days=30))].drop(columns=["_k"])
    return bl, ba, rc, bd


# ── insights ──────────────────────────────────────────────────────────────────

def build_insights(df: pd.DataFrame) -> dict[str, str]:
    pend = df[df["Status Retorno"] == "Pendente de Retorno"].copy()
    pend["Data de Entrega"] = pd.to_datetime(pend["Data de Entrega"], errors="coerce")
    pend["dias"] = (pd.Timestamp.now().normalize() - pend["Data de Entrega"].dt.normalize()).dt.days

    top_ag = "Sem pendências no período."
    risk = (pend.groupby("Agente", as_index=False)["Logger"].count()
                .rename(columns={"Logger": "Pend"}).sort_values("Pend", ascending=False))
    if not risk.empty:
        top_ag = f"{risk.iloc[0]['Agente']} lidera com {fmt_int(risk.iloc[0]['Pend'])} loggers pendentes."

    uf_txt = "Sem UF com volume mínimo para análise."
    uf_ev = df.copy()
    uf_ev["_p"] = uf_ev["Status Retorno"].eq("Pendente de Retorno")
    uf_sc = uf_ev.groupby("UF Destino", as_index=False).agg(total=("Logger","count"), pend=("_p","sum"))
    uf_sc = uf_sc[uf_sc["total"] >= 20]
    if not uf_sc.empty:
        uf_sc["taxa"] = uf_sc.apply(lambda r: safe_pct(int(r["pend"]), int(r["total"])), axis=1)
        t = uf_sc.sort_values(["taxa","pend"], ascending=False).iloc[0]
        uf_txt = f"UF {t['UF Destino']} com {fmt_pct(t['taxa'])} de pendência ({fmt_int(t['pend'])}/{fmt_int(t['total'])})."

    oldest_txt = "Não há pendências abertas."
    if not pend.empty and pend["dias"].notna().any():
        o = pend.sort_values("dias", ascending=False).iloc[0]
        oldest_txt = f"Pedido {o['Pedido']} · logger {o['Logger']} há {fmt_int(o['dias'])} dias sem retorno."

    rec7 = df.copy()
    rec7["Ultimo_Historico"] = pd.to_datetime(rec7["Ultimo_Historico"], errors="coerce")
    start7 = pd.Timestamp.now().normalize() - pd.Timedelta(days=6)
    end7   = pd.Timestamp.now().normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    rec7 = rec7[rec7["Ultimo_Historico"].between(start7, end7)]
    rec7_txt = "Nenhum retorno nos últimos 7 dias."
    if not rec7.empty:
        n = int(rec7["Logger"].fillna("").astype(str).str.strip().loc[lambda s: s != ""].nunique())
        if n:
            rec7_txt = f"{fmt_int(n)} loggers recebidos no somatório dos últimos 7 dias."

    return {"Agente em Atenção": top_ag, "UF Crítica": uf_txt,
            "Pendência Mais Antiga": oldest_txt, "Retorno Recente": rec7_txt}


# ── chart helpers ─────────────────────────────────────────────────────────────

def _style(fig, height=320):
    fig.update_layout(
        paper_bgcolor="#141b26", plot_bgcolor="#141b26", title_text="",
        font={"color": "#e8edf5"},
        xaxis={"gridcolor":"#2b3a4d","zerolinecolor":"#2b3a4d","linecolor":"#2b3a4d","tickfont":{"color":"#d3dceb"}},
        yaxis={"gridcolor":"#2b3a4d","zerolinecolor":"#2b3a4d","linecolor":"#2b3a4d","tickfont":{"color":"#d3dceb"}},
        hoverlabel={"bgcolor":"#1b2635","font":{"color":"#f1f5fb"}},
        margin=dict(l=10, r=40, t=20, b=10), height=height,
    )
    fig.update_xaxes(title_text="")
    fig.update_yaxes(title_text="")

def _bar_h(data: pd.Series, color: str, height=320) -> str:
    df = data.reset_index()
    df.columns = ["y", "x"]
    fig = px.bar(df, x="x", y="y", orientation="h", text="x",
                 color_discrete_sequence=[color])
    fig.update_traces(textposition="outside", cliponaxis=False)
    _style(fig, height)
    fig.update_layout(yaxis={"autorange":"reversed"})
    return _fig_div(fig)

def _bar_v(data: pd.Series, color: str) -> str:
    df = data.reset_index()
    df.columns = ["x", "y"]
    df["x"] = df["x"].astype(str)
    fig = px.bar(df, x="x", y="y", text="y", color_discrete_sequence=[color])
    fig.update_traces(textposition="outside")
    fig.update_layout(bargap=0.25)
    _style(fig, 280)
    return _fig_div(fig)

def _line(data: pd.Series, color: str) -> str:
    df = data.reset_index()
    df.columns = ["Semana", "Loggers"]
    df["Semana"] = df["Semana"].astype(str)
    fig = px.line(df, x="Semana", y="Loggers", markers=True,
                  color_discrete_sequence=[color])
    _style(fig, 280)
    return _fig_div(fig)


# ── CSS (idêntico ao dashboard, adaptado para HTML) ───────────────────────────

CSS = """
<style>
*{box-sizing:border-box;margin:0;padding:0;}
:root{--bg-main:#0b1020;--bg-panel:#121b2d;--bg-panel-2:#0f1728;--line:#24344d;
      --txt:#e8edf5;--txt-soft:#9fb0c8;--brand:#2f80d0;--brand-2:#1d4f8f;}
body{background:radial-gradient(900px 260px at 0% -10%,#13233f 0%,rgba(19,35,63,0) 60%),
     radial-gradient(700px 220px at 100% -20%,#1a2d4d 0%,rgba(26,45,77,0) 58%),
     var(--bg-main);color:var(--txt);font-family:"Segoe UI","Trebuchet MS",sans-serif;padding:18px;}
.hero-wrap{background:linear-gradient(120deg,#0f2344 0%,#173463 52%,#1e4178 100%);
  border:1px solid #2b4a76;border-radius:16px;padding:14px 18px;margin-bottom:14px;}
.hero-title{margin:0;font-size:2rem;font-weight:800;}
.hero-sub{margin-top:4px;color:#bdd0ec;font-size:.95rem;}
.caption{font-size:.78rem;color:#7a90a8;margin-bottom:10px;}
.period-bar{display:flex;align-items:center;gap:8px;margin:0 0 14px;}
.period-label{font-size:.85rem;color:#9fb7d4;font-weight:600;margin-right:4px;}
.period-btn{background:#13243c;border:1px solid #2b466b;border-radius:8px;
  color:#9fb7d4;cursor:pointer;font-size:.85rem;font-weight:600;padding:6px 16px;
  transition:background .15s,color .15s,border-color .15s;}
.period-btn:hover{background:#1a3358;color:#dceafe;border-color:#3a6090;}
.period-btn.active{background:linear-gradient(90deg,#1d4f8f,#2f80d0);
  border-color:#3a80d0;color:#fff;}
.kpi-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:14px;margin:6px 0 16px;}
.kpi-card{position:relative;overflow:hidden;
  background:linear-gradient(155deg,#15233a 0%,#121b2d 62%,#101829 100%);
  border:1px solid #2a3e5e;border-radius:14px;padding:16px 18px;min-height:108px;
  box-shadow:inset 0 1px 0 rgba(125,173,230,.06);text-align:center;}
.kpi-card::before{content:"";position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,#3f7bc3 0%,#75b5ff 100%);opacity:.85;}
.kpi-title{font-size:.9rem;font-weight:700;color:#9fb7d4;margin-bottom:12px;}
.kpi-value{font-size:2.2rem;font-weight:800;color:#f6fbff;line-height:1;}
.insight-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:8px 0 12px;}
.insight-card{background:linear-gradient(170deg,#13243c 0%,#101b2d 100%);
  border:1px solid #2b466b;border-radius:12px;padding:12px 14px;}
.insight-title{font-size:.88rem;font-weight:700;color:#9ec2ea;margin-bottom:6px;}
.insight-text{font-size:.82rem;color:#c8daf2;line-height:1.5;}
.section-head{display:flex;align-items:baseline;justify-content:space-between;margin:14px 2px 8px;}
.section-title{font-size:1.02rem;font-weight:800;color:#dceafe;}
.section-note{font-size:.78rem;color:#96afcf;}
.charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin-top:8px;}
.charts-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;margin-top:8px;}
.chart-box{background:#141b26;border:1px solid #24344d;border-radius:12px;padding:10px;}
.chart-title{font-size:.92rem;font-weight:700;color:#dceafe;margin-bottom:4px;padding:0 4px;}
.chart-empty{display:flex;align-items:center;justify-content:center;min-height:220px;
  border:1px dashed #2b466b;border-radius:10px;color:#7f93ad;
  background:rgba(19,36,60,.35);font-size:.9rem;text-align:center;padding:12px;}
footer{margin-top:20px;font-size:.75rem;color:#556070;text-align:right;}
@media(max-width:1200px){
  .kpi-grid{grid-template-columns:repeat(3,minmax(0,1fr));}
  .insight-grid{grid-template-columns:repeat(2,minmax(0,1fr));}
  .charts-3{grid-template-columns:repeat(2,minmax(0,1fr));}
}
@media(max-width:720px){
  .kpi-grid,.insight-grid,.charts,.charts-3{grid-template-columns:1fr;}
}
</style>
"""


# ── per-period data computation ───────────────────────────────────────────────

def _compute_period_data(df: pd.DataFrame) -> dict:
    pedidos    = df["Pedido"].nunique()
    loggers    = len(df)
    retornados = int((df["Status Retorno"] == "Retornado").sum())
    pendentes  = int((df["Status Retorno"] == "Pendente de Retorno").sum())
    taxa_ret   = safe_pct(retornados, loggers)
    taxa_pend  = safe_pct(pendentes, loggers)

    insights = build_insights(df)

    top_ag_s = (df.groupby("Agente")["Logger"].count()
                .sort_values(ascending=True).tail(12))
    top_pend_s = (df[df["Status Retorno"] == "Pendente de Retorno"]
                  .groupby("Agente")["Logger"].count()
                  .sort_values(ascending=True).tail(12))
    trend_s = (df.set_index("_data_entrega_dt").resample("W-MON")["Logger"].count())
    uf_s = (df.groupby("UF Destino")["Logger"].count()
            .sort_values(ascending=True).tail(10))

    m7 = df.copy()
    m7["Ultimo_Historico"] = pd.to_datetime(m7["Ultimo_Historico"], errors="coerce")
    start7 = pd.Timestamp.now().normalize() - pd.Timedelta(days=6)
    end7   = pd.Timestamp.now().normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    m7 = m7[m7["Ultimo_Historico"].between(start7, end7)]
    m7["Dia"] = m7["Ultimo_Historico"].dt.strftime("%d/%m")
    rec7_s = m7.groupby("Dia")["Logger"].nunique()
    all_days = [(pd.Timestamp.now().normalize() - pd.Timedelta(days=i)).strftime("%d/%m")
                for i in range(6, -1, -1)]
    rec7_s = rec7_s.reindex(all_days, fill_value=0)

    return {
        "kpi": {
            "pedidos":    fmt_int(pedidos),
            "loggers":    fmt_int(loggers),
            "retornados": fmt_int(retornados),
            "pendentes":  fmt_int(pendentes),
            "taxa_ret":   fmt_pct(taxa_ret),
            "taxa_pend":  fmt_pct(taxa_pend),
        },
        "insights": {
            "agente":  insights.get("Agente em Atenção", ""),
            "uf":      insights.get("UF Crítica", ""),
            "antiga":  insights.get("Pendência Mais Antiga", ""),
            "recente": insights.get("Retorno Recente", ""),
        },
        "charts": {
            "ag":    {"y": top_ag_s.index.tolist(),   "x": [int(v) for v in top_ag_s.values.tolist()]},
            "pend":  {"y": top_pend_s.index.tolist(), "x": [int(v) for v in top_pend_s.values.tolist()]},
            "trend": {"x": [str(i) for i in trend_s.index.tolist()], "y": [int(v) for v in trend_s.values.tolist()]},
            "uf":    {"y": uf_s.index.tolist(),       "x": [int(v) for v in uf_s.values.tolist()]},
            "rec7":  {"x": rec7_s.index.tolist(),     "y": [int(v) for v in rec7_s.values.tolist()]},
        },
    }


# ── generate_html ─────────────────────────────────────────────────────────────

def generate_html(model_full: pd.DataFrame) -> str:
    periods_data: dict[int, dict] = {}
    for days in PERIODOS:
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        df_p = model_full[model_full["_data_entrega_dt"] >= cutoff].copy()
        periods_data[days] = _compute_period_data(df_p)
        print(f"  [reversa] Período {days}d: {len(df_p)} registros")

    pad = periods_data[PERIODO_PAD]
    gerado = datetime.now().strftime("%d/%m/%Y %H:%M")
    periods_json = json.dumps({str(k): v for k, v in periods_data.items()},
                              ensure_ascii=False, default=str)

    btn_labels = {7: "Últimos 7 dias", 30: "Últimos 30 dias", 60: "Últimos 60 dias"}
    btns_html = "".join(
        f'<button class="period-btn{" active" if d == PERIODO_PAD else ""}" '
        f'id="btn-{d}" onclick="switchPeriod({d})">{btn_labels[d]}</button>'
        for d in PERIODOS
    )

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Controle de Reversa — Dataloggers</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
{CSS}
</head>
<body>

<div class="hero-wrap">
  <h1 class="hero-title">Controle de Reversa de Dataloggers</h1>
  <div class="hero-sub">Gerado em: {gerado} &nbsp;·&nbsp; VTC LOG — BI Qualidade</div>
</div>

<div class="period-bar">
  <span class="period-label">Período:</span>
  {btns_html}
</div>

<div class="kpi-grid">
  <div class="kpi-card"><div class="kpi-title">Pedidos Entregues</div><div class="kpi-value" id="kpi-pedidos">{pad["kpi"]["pedidos"]}</div></div>
  <div class="kpi-card"><div class="kpi-title">Loggers Entregues</div><div class="kpi-value" id="kpi-loggers">{pad["kpi"]["loggers"]}</div></div>
  <div class="kpi-card"><div class="kpi-title">Loggers Retornados</div><div class="kpi-value" id="kpi-retornados">{pad["kpi"]["retornados"]}</div></div>
  <div class="kpi-card"><div class="kpi-title">Loggers Pendentes</div><div class="kpi-value" id="kpi-pendentes">{pad["kpi"]["pendentes"]}</div></div>
  <div class="kpi-card"><div class="kpi-title">Taxa de Retorno</div><div class="kpi-value" id="kpi-taxa-ret">{pad["kpi"]["taxa_ret"]}</div></div>
  <div class="kpi-card"><div class="kpi-title">Taxa de Pendência</div><div class="kpi-value" id="kpi-taxa-pend">{pad["kpi"]["taxa_pend"]}</div></div>
</div>

<div class="section-head">
  <h3 class="section-title">Insights do Período</h3>
</div>
<div class="insight-grid">
  <div class="insight-card"><div class="insight-title">Agente em Atenção</div><div class="insight-text" id="ins-agente">{pad["insights"]["agente"]}</div></div>
  <div class="insight-card"><div class="insight-title">UF Crítica</div><div class="insight-text" id="ins-uf">{pad["insights"]["uf"]}</div></div>
  <div class="insight-card"><div class="insight-title">Pendência Mais Antiga</div><div class="insight-text" id="ins-antiga">{pad["insights"]["antiga"]}</div></div>
  <div class="insight-card"><div class="insight-title">Retorno Recente</div><div class="insight-text" id="ins-recente">{pad["insights"]["recente"]}</div></div>
</div>

<div class="section-head">
  <h3 class="section-title">Loggers por Agente</h3>
  <p class="section-note">Top 12</p>
</div>
<div class="charts">
  <div class="chart-box">
    <div class="chart-title">Expedidos por Agente</div>
    <div id="chart-ag" style="height:320px"></div>
  </div>
  <div class="chart-box">
    <div class="chart-title">Pendentes de Retorno por Agente</div>
    <div id="chart-pend" style="height:320px"></div>
  </div>
</div>

<div class="section-head">
  <h3 class="section-title">Tendência e Distribuição</h3>
</div>
<div class="charts-3">
  <div class="chart-box">
    <div class="chart-title">Tendência Semanal</div>
    <div id="chart-trend" style="height:280px"></div>
  </div>
  <div class="chart-box">
    <div class="chart-title">Loggers por UF Destino (Top 10)</div>
    <div id="chart-uf" style="height:280px"></div>
  </div>
  <div class="chart-box">
    <div class="chart-title">Dispositivos Recebidos — Últimos 7 dias</div>
    <div id="chart-rec7" style="height:280px"></div>
  </div>
</div>

<footer>Atualizado automaticamente a cada 10 min &nbsp;·&nbsp; VTC LOG — BI Qualidade</footer>

<script>
const PERIODS_DATA = {periods_json};

const LAYOUT_BASE = {{
  paper_bgcolor:"#141b26", plot_bgcolor:"#141b26",
  font:{{color:"#e8edf5"}},
  xaxis:{{gridcolor:"#2b3a4d",zerolinecolor:"#2b3a4d",linecolor:"#2b3a4d",tickfont:{{color:"#d3dceb"}}}},
  yaxis:{{gridcolor:"#2b3a4d",zerolinecolor:"#2b3a4d",linecolor:"#2b3a4d",tickfont:{{color:"#d3dceb"}}}},
  hoverlabel:{{bgcolor:"#1b2635",font:{{color:"#f1f5fb"}}}},
  margin:{{l:10,r:50,t:20,b:10}},
  showlegend:false
}};

function layoutH(height){{
  return Object.assign({{}}, LAYOUT_BASE, {{
    height:height,
    yaxis:Object.assign({{}},LAYOUT_BASE.yaxis,{{autorange:"reversed"}})
  }});
}}
function layoutV(height){{
  return Object.assign({{}}, LAYOUT_BASE, {{height:height}});
}}

function traceBarH(d, color){{
  return [{{type:"bar",x:d.x,y:d.y,orientation:"h",text:d.x,
            textposition:"outside",cliponaxis:false,
            marker:{{color:color}}}}];
}}
function traceBarV(d, color){{
  return [{{type:"bar",x:d.x,y:d.y,text:d.y,textposition:"outside",
            marker:{{color:color}}}}];
}}
function traceLine(d, color){{
  return [{{type:"scatter",x:d.x,y:d.y,mode:"lines+markers",
            line:{{color:color,width:2}},marker:{{color:color,size:6}}}}];
}}

const cfg = {{displayModeBar:false, responsive:true}};

function hasSeriesData(series){{
  return !!series && (
    (Array.isArray(series.x) && series.x.length > 0) ||
    (Array.isArray(series.y) && series.y.length > 0)
  );
}}

function renderChart(id, traces, layout){{
  const el = document.getElementById(id);
  if (!el) return;
  if (!traces || !traces.length || !traces.some(hasSeriesData)) {{
    el.innerHTML = '<div class="chart-empty">Sem dados no período selecionado</div>';
    return;
  }}
  if (window.Plotly && Plotly.purge) {{
    Plotly.purge(el);
  }}
  el.innerHTML = "";
  requestAnimationFrame(() => {{
    Promise.resolve(Plotly.newPlot(el, traces, layout, cfg))
      .then(() => {{
        if (Plotly.Plots && Plotly.Plots.resize) {{
          Plotly.Plots.resize(el);
        }}
      }})
      .catch((err) => {{
        console.error(`Falha ao renderizar ${{id}}:`, err);
        el.innerHTML = '<div class="chart-empty">Não foi possível carregar este gráfico</div>';
      }});
  }});
}}

function renderAll(days){{
  const d = PERIODS_DATA[String(days)] || PERIODS_DATA[String(PERIODO_PAD)];
  if (!d) return;

  // KPIs
  document.getElementById("kpi-pedidos").textContent    = d.kpi.pedidos;
  document.getElementById("kpi-loggers").textContent    = d.kpi.loggers;
  document.getElementById("kpi-retornados").textContent = d.kpi.retornados;
  document.getElementById("kpi-pendentes").textContent  = d.kpi.pendentes;
  document.getElementById("kpi-taxa-ret").textContent   = d.kpi.taxa_ret;
  document.getElementById("kpi-taxa-pend").textContent  = d.kpi.taxa_pend;

  // Insights
  document.getElementById("ins-agente").textContent  = d.insights.agente;
  document.getElementById("ins-uf").textContent      = d.insights.uf;
  document.getElementById("ins-antiga").textContent  = d.insights.antiga;
  document.getElementById("ins-recente").textContent = d.insights.recente;

  // Charts
  renderChart("chart-ag",    traceBarH(d.charts.ag,   "#144b8b"), layoutH(320));
  renderChart("chart-pend",  traceBarH(d.charts.pend, "#1f6cb8"), layoutH(320));
  renderChart("chart-trend", traceLine(d.charts.trend, "#2f80d0"), layoutV(280));
  renderChart("chart-uf",    traceBarH(d.charts.uf,   "#144b8b"), layoutH(280));
  renderChart("chart-rec7",  traceBarV(d.charts.rec7, "#1f6cb8"), layoutV(280));
}}

function switchPeriod(days){{
  document.querySelectorAll(".period-btn").forEach(b => b.classList.remove("active"));
  document.getElementById("btn-"+days).classList.add("active");
  renderAll(days);
}}

window.addEventListener("resize", () => {{
  ["chart-ag", "chart-pend", "chart-trend", "chart-uf", "chart-rec7"].forEach((id) => {{
    const el = document.getElementById(id);
    if (el && Plotly.Plots && Plotly.Plots.resize) {{
      Plotly.Plots.resize(el);
    }}
  }});
}});

// Initial render
renderAll(PERIODO_PAD);
</script>
</body>
</html>"""


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("  [reversa] Carregando snapshots...")
    base_loggers       = pd.read_pickle(SNAPSHOT_DIR / "base_loggers.pkl")
    base_agentes       = pd.read_pickle(SNAPSHOT_DIR / "base_agentes.pkl")
    recebimento        = pd.read_pickle(SNAPSHOT_DIR / "recebimento.pkl")
    base_destinatarios = pd.read_pickle(SNAPSHOT_DIR / "base_destinatarios.pkl")

    max_days = max(PERIODOS)
    base_loggers, base_agentes, recebimento, base_destinatarios = _prefilter(
        base_loggers, base_agentes, recebimento, base_destinatarios, max_days
    )
    print(f"  [reversa] Pós-filtro ({max_days}d): loggers={len(base_loggers)} agentes={len(base_agentes)} receb={len(recebimento)}")

    model_full = build_model(base_loggers, base_agentes, recebimento, base_destinatarios)
    cutoff_max = pd.Timestamp.now() - pd.Timedelta(days=max_days)
    model_full = model_full[model_full["_data_entrega_dt"] >= cutoff_max].copy()
    print(f"  [reversa] Modelo completo: {len(model_full)} registros")

    html = generate_html(model_full)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"  [reversa] HTML salvo: {OUTPUT_FILE.name}")


if __name__ == "__main__":
    main()
