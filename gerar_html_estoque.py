"""Gera ESTOQUE_DATALOGGERS.html replicando fielmente o dashboard_loggers_estoque.py"""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import unicodedata
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.io as pio
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

OUTPUT_FILE = Path(__file__).resolve().parent / "ESTOQUE_DATALOGGERS.html"
APP_DIR = Path(__file__).resolve().parents[1] / "streamlit"
AUX_STATUS_FILE = APP_DIR / "TAG para Estoque.xlsx"

POSTGRES_CFG = {
    "host": "10.141.0.32", "port": 5432,
    "database": "dtbPortal", "user": "bi_qualidade", "password": ">RI477b:",
}
DESTINO_CAMARA_FRIA   = "6c1d1b0f-0613-47d3-9687-00f7bdef4980"
FINALIDADE_PEDIDOS    = "cf90119a-6db4-4b13-ac19-b71572913727"
DESTINO_ESTOQUE       = "25d5c356-32a8-4221-84ae-8230061b9163"
FINALIDADE_SALDO_EST  = "e8031b09-2d30-414d-af5b-16e43a41618b"
FINALIDADE_PACKING    = "460c2f9a-2d38-40c3-a9ff-58eabb6cb21f"
SNAPSHOT_CUTOFF       = "2024-06-26"
BASE_DAYS             = 7
DEVICE_TYPES          = {"ARES", "ARES COM SONDA", "SENSOR VTC", "SHIELD", "SYOS"}


# ── helpers ──────────────────────────────────────────────────────────────────

def normalize_text(value) -> str:
    s = str(value) if value is not None else ""
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return " ".join(s.strip().upper().split())

def normalize_tag_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()

def fmt(n: int) -> str:
    return f"{int(n):,}".replace(",", ".")

def _build_url():
    return URL.create("postgresql+psycopg2",
        username=POSTGRES_CFG["user"], password=POSTGRES_CFG["password"],
        host=POSTGRES_CFG["host"], port=POSTGRES_CFG["port"],
        database=POSTGRES_CFG["database"])

def _read(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)

def _fig_div(fig) -> str:
    return pio.to_html(fig, include_plotlyjs=False, full_html=False,
                       config={"displayModeBar": False})


# ── queries (idênticas ao dashboard) ─────────────────────────────────────────

def q_estoque_geral() -> str:
    return """
SELECT ped.cd_ufdestino, vwt.ds_tipodatalogger, vwt.ds_tag,
       vwt.ds_destino, vwt.ds_finalidade, vwt.ds_responsavel,
       vwt.id_usuarioatualizacao::text, vwt.dt_atualizacao, vwt.ds_statusrecebimento
FROM vwTabelaMovDataloggers vwt
LEFT JOIN tbdsemembalagens emb ON vwt.cd_embalagem::text = emb.cd_embalagem::text
LEFT JOIN vwimportacaopedidos ped ON emb.id_sem = ped.id_sem
WHERE vwt.dt_atualizacao >= CURRENT_DATE - INTERVAL '120 days'"""


def _q_hist_base(where: str, days: int) -> str:
    return f"""
SELECT dh.dt_inclusao AS dt_historico, tam.text AS ds_acaomovimentacao,
       dd.ds_destino, df.ds_finalidade, dh.nr_historico,
       d.nr_datalogger, d.ds_datalogger, d.ds_tag, d.ds_serie, dh.ds_observacao,
       (ui.ds_nome::text || ' ' || ui.ds_sobrenome::text) AS ds_usuarioinclusao
FROM public.tbddataloggerhistoricos dh
INNER JOIN vwtipos tam ON ds_tipo='tipoacaomovimentacao' AND dh.tp_acaomovimentacao=tam.id
INNER JOIN public.tbdcaddataloggerdestinos dd ON dh.id_destino=dd.id_destino
INNER JOIN public.tbdcaddataloggerfinalidades df ON dh.id_finalidade=df.id_finalidade
INNER JOIN tbdcaddataloggers d ON dh.id_datalogger=d.id_datalogger
LEFT JOIN vwusuarios ui ON dh.id_usuarioinclusao=ui.id_usuario
WHERE {where}
  AND dh.dt_inclusao >= GREATEST(TIMESTAMP '{SNAPSHOT_CUTOFF}',
        CURRENT_DATE - INTERVAL '{days} days')"""

def q_mov_cf(days: int) -> str:
    return _q_hist_base(
        f"dh.tp_acaomovimentacao=1 AND dh.id_destino='{DESTINO_CAMARA_FRIA}' AND dh.id_finalidade='{FINALIDADE_PEDIDOS}'",
        days)

def q_rec_est(days: int) -> str:
    return _q_hist_base(
        f"dh.tp_acaomovimentacao IN (2,12) AND dh.id_destino='{DESTINO_ESTOQUE}' AND dh.id_finalidade='{FINALIDADE_SALDO_EST}'",
        days)

def q_packing(days: int) -> str:
    return _q_hist_base(
        f"dh.tp_acaomovimentacao NOT IN (3) AND dh.id_destino='{DESTINO_CAMARA_FRIA}' AND dh.id_finalidade='{FINALIDADE_PACKING}'",
        days)

def q_mov_recente(days: int) -> str:
    return f"""
SELECT d.ds_tag, dh.dt_inclusao AS dt_historico
FROM public.tbddataloggerhistoricos dh
INNER JOIN tbdcaddataloggers d ON dh.id_datalogger=d.id_datalogger
WHERE dh.dt_inclusao >= CURRENT_DATE - INTERVAL '{days} days'"""


# ── aux status mapping ────────────────────────────────────────────────────────

def load_aux_map() -> pd.DataFrame:
    if not AUX_STATUS_FILE.exists():
        return pd.DataFrame(columns=["_map_key", "situacao_oficial"])
    aux = pd.read_excel(AUX_STATUS_FILE)
    if aux.empty:
        return pd.DataFrame(columns=["_map_key", "situacao_oficial"])
    col_map = {normalize_text(c): c for c in aux.columns}
    req = ["DS_DESTINO", "DS_STATUSRECEBIMENTO", "STATUS", "SITUACAO"]
    if not all(k in col_map for k in req):
        return pd.DataFrame(columns=["_map_key", "situacao_oficial"])
    work = aux.rename(columns={
        col_map["DS_DESTINO"]: "ds_destino_aux",
        col_map["DS_STATUSRECEBIMENTO"]: "ds_finalidade_aux",
        col_map["STATUS"]: "status_recebimento_aux",
        col_map["SITUACAO"]: "situacao_oficial",
    }).copy()
    for c in ["ds_destino_aux", "ds_finalidade_aux", "status_recebimento_aux"]:
        work[c] = work[c].map(normalize_text)
    work["_map_key"] = work["ds_destino_aux"] + "|" + work["ds_finalidade_aux"] + "|" + work["status_recebimento_aux"]
    work["situacao_oficial"] = work["situacao_oficial"].fillna("Sem Mapeamento").astype(str).str.strip()
    return work[["_map_key", "situacao_oficial"]].drop_duplicates(subset=["_map_key"], keep="first")

def apply_aux_status(df: pd.DataFrame, aux_map: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["situacao_oficial"] = pd.Series(dtype="object")
        return out
    out["_map_key"] = (
        out.get("ds_destino",        pd.Series(index=out.index, dtype="object")).map(normalize_text) + "|" +
        out.get("ds_finalidade",     pd.Series(index=out.index, dtype="object")).map(normalize_text) + "|" +
        out.get("ds_statusrecebimento", pd.Series(index=out.index, dtype="object")).map(normalize_text)
    )
    if aux_map.empty:
        out["situacao_oficial"] = "Sem Mapeamento"
        return out
    out = out.merge(aux_map, how="left", on="_map_key")
    out["situacao_oficial"] = out["situacao_oficial"].fillna("Sem Mapeamento")
    return out


# ── daily_unique (idêntico ao dashboard) ─────────────────────────────────────

def daily_unique(df: pd.DataFrame, date_col: str, id_col: str = "ds_tag", last_n_days: int = 7) -> pd.DataFrame:
    full_days = pd.DataFrame({"Dia": pd.date_range(end=pd.Timestamp.now().normalize(), periods=last_n_days, freq="D")})
    if df.empty or date_col not in df.columns or id_col not in df.columns:
        full_days["Total"] = 0
        full_days["Dia"] = full_days["Dia"].dt.date
        return full_days
    base = df.copy()
    base["Dia"] = pd.to_datetime(base[date_col], errors="coerce").dt.normalize()
    base[id_col] = base[id_col].fillna("").astype(str).str.strip()
    base = base[(base["Dia"].notna()) & (base[id_col] != "")]
    agg = (base.groupby("Dia", as_index=False)[id_col].nunique()
               .rename(columns={id_col: "Total"}).sort_values("Dia"))
    out = full_days.merge(agg, how="left", on="Dia")
    out["Total"] = out["Total"].fillna(0).astype(int)
    out["Dia"] = out["Dia"].dt.date
    return out

def get_today_delta(df_daily: pd.DataFrame) -> tuple[int, int]:
    if df_daily.empty:
        return 0, 0
    today = pd.Timestamp.now().normalize().date()
    yesterday = (pd.Timestamp.now().normalize() - pd.Timedelta(days=1)).date()
    t = int(df_daily.loc[df_daily["Dia"].eq(today), "Total"].sum())
    y = int(df_daily.loc[df_daily["Dia"].eq(yesterday), "Total"].sum())
    return t, t - y


# ── charts ────────────────────────────────────────────────────────────────────

def _style(fig, height=300):
    fig.update_layout(
        paper_bgcolor="#141b26", plot_bgcolor="#141b26",
        title_text="", font={"color": "#e8edf5"},
        xaxis={"gridcolor": "#2b3a4d", "zerolinecolor": "#2b3a4d",
               "linecolor": "#2b3a4d", "tickfont": {"color": "#d3dceb"}},
        yaxis={"gridcolor": "#2b3a4d", "zerolinecolor": "#2b3a4d",
               "linecolor": "#2b3a4d", "tickfont": {"color": "#d3dceb"}},
        hoverlabel={"bgcolor": "#1b2635", "font": {"color": "#f1f5fb"}},
        margin=dict(l=10, r=40, t=20, b=10), height=height,
    )
    fig.update_xaxes(title_text="")
    fig.update_yaxes(title_text="")

def chart_bar_day(df_daily: pd.DataFrame, color: str) -> str:
    df = df_daily.copy()
    df["Dia"] = df["Dia"].astype(str).str[-5:]   # dd-mm
    fig = px.bar(df, x="Dia", y="Total", text="Total",
                 color_discrete_sequence=[color])
    fig.update_traces(textposition="outside")
    fig.update_layout(bargap=0.25)
    _style(fig, 290)
    return _fig_div(fig)

def chart_status(status_df: pd.DataFrame) -> str:
    top = status_df.head(12).sort_values("Total", ascending=True)
    fig = px.bar(top, x="Total", y="Status", orientation="h", text="Total",
                 color_discrete_sequence=["#274a9f"])
    fig.update_traces(textposition="outside", cliponaxis=False)
    _style(fig, 430)
    return _fig_div(fig)


# ── classify badge ────────────────────────────────────────────────────────────

def classify_badge(disponivel: int, aguardando: int):
    ratio_d = disponivel / max(aguardando, 1)
    ratio_a = aguardando / max(disponivel, 1)
    def _lbl(good, r):
        if r >= 1.2: return ("normal", "badge-normal") if good else ("critico", "badge-critical")
        if r >= 0.8: return ("atencao", "badge-warning")
        return ("critico", "badge-critical") if good else ("normal", "badge-normal")
    dl, dc = _lbl(True,  ratio_d)
    al, ac = _lbl(False, ratio_a)
    return dl, dc, al, ac


# ── CSS (idêntico ao dashboard, adaptado para HTML puro) ─────────────────────

CSS = """
<style>
*{box-sizing:border-box;margin:0;padding:0;}
:root{--bg-main:#0b1020;--bg-panel:#121b2d;--bg-panel-2:#0f1728;--line:#24344d;}
body{background:radial-gradient(900px 260px at 0% -10%,#13233f 0%,rgba(19,35,63,0) 60%),
     radial-gradient(700px 220px at 100% -20%,#1a2d4d 0%,rgba(26,45,77,0) 58%),
     var(--bg-main);color:#e8edf5;font-family:"Segoe UI","Trebuchet MS",sans-serif;padding:18px;}
.hero-wrap{background:linear-gradient(120deg,#0f2344 0%,#173463 52%,#1e4178 100%);
  border:1px solid #2b4a76;border-radius:16px;padding:14px 18px;margin-bottom:14px;}
.hero-title{margin:0;font-size:2rem;font-weight:800;}
.hero-sub{margin-top:4px;color:#bdd0ec;font-size:.95rem;}
.hero-meta{margin-top:6px;display:flex;flex-wrap:wrap;gap:8px;}
.hero-pill{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;
  background:rgba(9,15,28,.34);border:1px solid rgba(173,200,232,.2);color:#dfeaf8;font-size:.78rem;}
.caption{font-size:.78rem;color:#7a90a8;margin-bottom:10px;}
.section-head{display:flex;align-items:baseline;justify-content:space-between;margin:10px 2px 8px 2px;}
.section-title{margin:0;font-size:1.02rem;font-weight:800;color:#dceafe;letter-spacing:.01em;}
.section-note{margin:0;font-size:.78rem;color:#96afcf;}
.kpi-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:6px 0 16px 0;}
.kpi-card{position:relative;overflow:hidden;
  background:linear-gradient(155deg,#15233a 0%,#121b2d 62%,#101829 100%);
  border:1px solid #2a3e5e;border-radius:14px;padding:16px 18px;min-height:108px;
  box-shadow:inset 0 1px 0 rgba(125,173,230,.06);text-align:center;}
.kpi-card::before{content:"";position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,#3f7bc3 0%,#75b5ff 100%);opacity:.85;}
.kpi-title{font-size:1rem;font-weight:700;color:#9fb7d4;margin-bottom:12px;}
.kpi-value{font-size:2.4rem;font-weight:800;color:#f6fbff;line-height:1;}
.resumo-wrap{background:linear-gradient(170deg,#121f34 0%,#0f1828 100%);
  border:1px solid #2a3e5e;border-radius:14px;padding:12px 12px 10px 12px;margin:8px 0 14px 0;}
.resumo-title{font-size:.98rem;font-weight:800;color:#d8e6fb;margin-bottom:9px;}
.resumo-grid-main{display:grid;grid-template-columns:repeat(3,minmax(240px,360px));
  justify-content:center;gap:12px;margin-bottom:12px;}
.resumo-card{position:relative;overflow:hidden;border:1px solid #2b466b;border-radius:12px;
  padding:14px;min-height:108px;
  background:linear-gradient(155deg,#15233a 0%,#121b2d 62%,#101829 100%);
  text-align:center;box-shadow:inset 0 1px 0 rgba(125,173,230,.06);}
.resumo-card::before{content:"";position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,#3f7bc3 0%,#75b5ff 100%);opacity:.85;}
.resumo-label{font-size:.96rem;color:#9ec2ea;font-weight:700;margin-bottom:10px;}
.resumo-value{font-size:2rem;color:#f1f6ff;font-weight:800;line-height:1.1;}
.resumo-meta{margin-top:3px;font-size:.72rem;color:#89a9cf;}
.critical-strip{display:grid;grid-template-columns:repeat(2,minmax(280px,420px));
  justify-content:center;gap:12px;margin:6px 0 12px 0;}
.critical-card{position:relative;overflow:hidden;border:1px solid #365379;border-radius:12px;
  padding:13px 14px;min-height:104px;
  background:linear-gradient(155deg,#15233a 0%,#121b2d 62%,#101829 100%);
  text-align:center;box-shadow:inset 0 1px 0 rgba(125,173,230,.06);}
.critical-card::before{content:"";position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,#3f7bc3 0%,#75b5ff 100%);opacity:.9;}
.critical-head{position:relative;display:flex;justify-content:center;align-items:center;
  margin-bottom:8px;min-height:20px;}
.critical-title{font-size:.95rem;color:#b9d1ee;font-weight:700;}
.critical-value{font-size:2rem;font-weight:800;color:#f5f9ff;line-height:1.1;}
.badge{position:absolute;right:0;top:0;font-size:.68rem;font-weight:800;text-transform:uppercase;
  letter-spacing:.04em;border-radius:999px;padding:3px 8px;border:1px solid transparent;}
.badge-normal{color:#9ff0c2;background:rgba(28,92,57,.5);border-color:rgba(134,237,185,.35);}
.badge-warning{color:#ffd48a;background:rgba(98,67,18,.5);border-color:rgba(245,195,109,.35);}
.badge-critical{color:#ffb3b3;background:rgba(96,31,31,.5);border-color:rgba(247,147,147,.35);}
.charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin-top:16px;}
.chart-box{background:#141b26;border:1px solid #24344d;border-radius:12px;padding:10px;}
.chart-title{font-size:.92rem;font-weight:700;color:#dceafe;margin-bottom:4px;padding:0 4px;}
footer{margin-top:20px;font-size:.75rem;color:#556070;text-align:right;}
@media(max-width:1100px){
  .kpi-grid{grid-template-columns:repeat(2,minmax(0,1fr));}
  .resumo-grid-main{grid-template-columns:repeat(2,minmax(240px,360px));}
  .charts{grid-template-columns:1fr;}
}
@media(max-width:640px){
  .kpi-grid,.resumo-grid-main,.critical-strip{grid-template-columns:1fr;}
}
</style>
"""


# ── HTML builder ──────────────────────────────────────────────────────────────

def generate_html(
    df_estoque_geral: pd.DataFrame,
    df_mov_cf: pd.DataFrame,
    df_rec_est: pd.DataFrame,
    df_packing: pd.DataFrame,
    df_mov_recente_5d: pd.DataFrame,
) -> str:

    # parse dates
    for df, col in [(df_estoque_geral, "dt_atualizacao"),
                    (df_mov_cf, "dt_historico"), (df_rec_est, "dt_historico"),
                    (df_packing, "dt_historico"), (df_mov_recente_5d, "dt_historico")]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    aux_map = load_aux_map()
    df_estoque_geral = apply_aux_status(df_estoque_geral, aux_map)

    # filtro de tipo de dispositivo
    if "ds_tipodatalogger" in df_estoque_geral.columns:
        df_estoque_geral["_tipo_norm"] = df_estoque_geral["ds_tipodatalogger"].map(normalize_text)
        df_estoque_geral = df_estoque_geral[df_estoque_geral["_tipo_norm"].isin(DEVICE_TYPES)].copy()

    # tag→tipo para filtrar históricos
    tag_tipo_map = pd.DataFrame(columns=["_tag_norm", "_tipo_norm"])
    if not df_estoque_geral.empty and {"ds_tag", "_tipo_norm"}.issubset(df_estoque_geral.columns):
        tag_tipo_map = (
            df_estoque_geral[["ds_tag", "_tipo_norm"]].copy()
            .assign(_tag_norm=lambda d: normalize_tag_series(d["ds_tag"]))
            .dropna(subset=["_tipo_norm"])
            .drop_duplicates(subset=["_tag_norm"], keep="first")[["_tag_norm", "_tipo_norm"]]
        )

    def _filter_hist(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if out.empty or "ds_tag" not in out.columns:
            return out
        out["_tag_norm"] = normalize_tag_series(out["ds_tag"])
        if not tag_tipo_map.empty:
            out = out.merge(tag_tipo_map, how="left", on="_tag_norm")
        if "_tipo_norm" in out.columns:
            out = out[out["_tipo_norm"].isin(DEVICE_TYPES)].copy()
        return out

    df_mov_cf        = _filter_hist(df_mov_cf)
    df_rec_est       = _filter_hist(df_rec_est)
    df_packing       = _filter_hist(df_packing)
    df_mov_recente_5d = _filter_hist(df_mov_recente_5d)

    # totais do período
    total_mov_cf  = int(df_mov_cf["ds_tag"].nunique())  if "ds_tag" in df_mov_cf.columns  else 0
    total_rec_est = int(df_rec_est["ds_tag"].nunique()) if "ds_tag" in df_rec_est.columns else 0
    total_packing = int(df_packing["ds_tag"].nunique()) if "ds_tag" in df_packing.columns else 0

    # estoque atual (ESTOQUE - GRU)
    total_estoque = 0
    if not df_estoque_geral.empty and "situacao_oficial" in df_estoque_geral.columns:
        est_base = df_estoque_geral.copy()
        est_base["_sit_norm"] = est_base["situacao_oficial"].map(normalize_text)
        est_base = est_base[est_base["_sit_norm"].eq("ESTOQUE - GRU")]
        if "ds_tag" in est_base.columns:
            total_estoque = int(est_base["ds_tag"].dropna().astype(str).str.strip()
                                .loc[lambda s: s.ne("")].nunique())

    # resumo do dia
    ret_dia     = daily_unique(df_rec_est, "dt_historico")
    cf_dia      = daily_unique(df_mov_cf,  "dt_historico")
    packing_dia = daily_unique(df_packing, "dt_historico")
    resumo_ret, _ = get_today_delta(ret_dia)
    resumo_cf,  _ = get_today_delta(cf_dia)
    resumo_emb, _ = get_today_delta(packing_dia)

    # critical strip: apto ao uso / aguardando recebimento (intersecção 5d)
    apto_uso = resumo_cf_aguar = 0
    if not df_estoque_geral.empty and {"situacao_oficial", "ds_tag"}.issubset(df_estoque_geral.columns):
        sit = df_estoque_geral["situacao_oficial"].fillna("").astype(str).str.casefold()
        for val, var_name in [("cf - apto ao uso", "apto"), ("cf - aguar. receber", "aguar")]:
            tags_set = {t for t in normalize_tag_series(df_estoque_geral.loc[sit.eq(val), "ds_tag"]) if t}
            if tags_set and "ds_tag" in df_mov_recente_5d.columns:
                mov_set = {t for t in normalize_tag_series(df_mov_recente_5d["ds_tag"]) if t}
                count = len(tags_set.intersection(mov_set))
                if var_name == "apto": apto_uso = count
                else: resumo_cf_aguar = count

    # status geral
    status_df = pd.DataFrame()
    map_coverage = 0.0
    if not df_estoque_geral.empty and "situacao_oficial" in df_estoque_geral.columns:
        s = df_estoque_geral["situacao_oficial"].fillna("Sem Mapeamento").astype(str).str.strip()
        status_df = s.value_counts().rename_axis("Status").reset_index(name="Total")
        map_coverage = (df_estoque_geral["situacao_oficial"] != "Sem Mapeamento").mean() * 100

    # badges
    dl, dc, al, ac = classify_badge(apto_uso, resumo_cf_aguar)
    def _badge(lbl, cls):
        return "" if lbl == "normal" else f'<div class="badge {cls}">{lbl}</div>'

    # charts
    ch_ret    = chart_bar_day(ret_dia,     "#4d7ed5")
    ch_cf     = chart_bar_day(cf_dia,      "#5c8ce2")
    ch_pack   = chart_bar_day(packing_dia, "#77a2ee")
    ch_status = chart_status(status_df) if not status_df.empty else "<p style='color:#8a9ab5'>Sem dados</p>"

    today_str = pd.Timestamp.now().strftime("%d/%m/%Y")
    gerado    = datetime.now().strftime("%d/%m/%Y %H:%M")
    ultima_atualizacao = pd.NaT
    if not df_estoque_geral.empty and "dt_atualizacao" in df_estoque_geral.columns:
        ultima_atualizacao = df_estoque_geral["dt_atualizacao"].max()
    ultima_atualizacao_str = (
        ultima_atualizacao.strftime("%d/%m/%Y %H:%M")
        if pd.notna(ultima_atualizacao)
        else "Sem dados"
    )

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Gestão de Dispositivos — Estoque e Câmara Fria</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
{CSS}
</head>
<body>

<div class="hero-wrap">
  <h1 class="hero-title">Gestão de Dispositivos</h1>
  <div class="hero-sub">Estoque, Câmara Fria e Volumes Embalados &nbsp;·&nbsp; Últimos {BASE_DAYS} dias &nbsp;·&nbsp; Gerado em {gerado}</div>
  <div class="hero-meta">
    <span class="hero-pill">Data da última atualização: {ultima_atualizacao_str}</span>
  </div>
</div>

<div class="critical-strip">
  <div class="critical-card">
    <div class="critical-head">
      <div class="critical-title">Disponível para Utilizar</div>
      {_badge(dl, dc)}
    </div>
    <div class="critical-value">{fmt(apto_uso)}</div>
    <div class="resumo-meta">Movimentação nos últimos 5 dias</div>
  </div>
  <div class="critical-card">
    <div class="critical-head">
      <div class="critical-title">Aguardando Recebimento</div>
      {_badge(al, ac)}
    </div>
    <div class="critical-value">{fmt(resumo_cf_aguar)}</div>
    <div class="resumo-meta">Movimentação nos últimos 5 dias</div>
  </div>
</div>

<div class="resumo-wrap">
  <div class="resumo-title">Resumo do Dia ({today_str})</div>
  <div class="resumo-grid-main">
    <div class="resumo-card">
      <div class="resumo-label">Entregues à Câmara Fria</div>
      <div class="resumo-value">{fmt(resumo_cf)}</div>
    </div>
    <div class="resumo-card">
      <div class="resumo-label">Volumes Embalados</div>
      <div class="resumo-value">{fmt(resumo_emb)}</div>
    </div>
    <div class="resumo-card">
      <div class="resumo-label">Retornados ao Estoque</div>
      <div class="resumo-value">{fmt(resumo_ret)}</div>
    </div>
  </div>
</div>

<div class="section-head">
  <h3 class="section-title">Indicadores Gerais</h3>
  <p class="section-note">Período: últimos {BASE_DAYS} dias</p>
</div>
<div class="kpi-grid">
  <div class="kpi-card"><div class="kpi-title">Retornados ao Estoque</div><div class="kpi-value">{fmt(total_rec_est)}</div></div>
  <div class="kpi-card"><div class="kpi-title">Entregues à Câmara Fria</div><div class="kpi-value">{fmt(total_mov_cf)}</div></div>
  <div class="kpi-card"><div class="kpi-title">Volumes Embalados</div><div class="kpi-value">{fmt(total_packing)}</div></div>
  <div class="kpi-card"><div class="kpi-title">Estoque Atual (GRU)</div><div class="kpi-value">{fmt(total_estoque)}</div></div>
</div>

<div class="section-head">
  <h3 class="section-title">Gráficos — Últimos 7 dias</h3>
  <p class="section-note">Cobertura do mapeamento auxiliar: {map_coverage:.1f}%</p>
</div>
<div class="charts">
  <div class="chart-box">
    <div class="chart-title">Retornados ao Estoque</div>
    {ch_ret}
  </div>
  <div class="chart-box">
    <div class="chart-title">Entregues à Câmara Fria</div>
    {ch_cf}
  </div>
  <div class="chart-box">
    <div class="chart-title">Status Geral dos Dataloggers (Top 12)</div>
    {ch_status}
  </div>
  <div class="chart-box">
    <div class="chart-title">Volumes Embalados</div>
    {ch_pack}
  </div>
</div>

<footer>Atualizado automaticamente a cada 10 min &nbsp;·&nbsp; VTC LOG — BI Qualidade</footer>
</body>
</html>"""


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("  [estoque] Conectando ao PostgreSQL...")
    engine = create_engine(_build_url(), pool_pre_ping=True,
                           connect_args={"options": "-c statement_timeout=60000"})

    print("  [estoque] Carregando dados em paralelo...")
    with ThreadPoolExecutor(max_workers=5) as ex:
        f_geral   = ex.submit(_read, engine, q_estoque_geral())
        f_cf      = ex.submit(_read, engine, q_mov_cf(BASE_DAYS))
        f_est     = ex.submit(_read, engine, q_rec_est(BASE_DAYS))
        f_pack    = ex.submit(_read, engine, q_packing(BASE_DAYS))
        f_recente = ex.submit(_read, engine, q_mov_recente(5))
        df_geral   = f_geral.result()
        df_cf      = f_cf.result()
        df_est     = f_est.result()
        df_pack    = f_pack.result()
        df_recente = f_recente.result()

    print(f"  [estoque] geral={len(df_geral)} cf={len(df_cf)} est={len(df_est)} pack={len(df_pack)} recente={len(df_recente)}")
    html = generate_html(df_geral, df_cf, df_est, df_pack, df_recente)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"  [estoque] HTML salvo: {OUTPUT_FILE.name}")


if __name__ == "__main__":
    main()
