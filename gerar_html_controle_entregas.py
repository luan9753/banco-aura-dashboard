from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio


WORKSPACE = Path(__file__).resolve().parent
SNAPSHOT_DIR = WORKSPACE.parent / "snapshot_reversa"
MODEL_FILE = SNAPSHOT_DIR / "modelo_final.pkl"
OUTPUT_HTML = WORKSPACE / "CONTROLE_ENTREGAS_20D.html"
OUTPUT_CSV = WORKSPACE / "CONTROLE_ENTREGAS_20D.csv"

WINDOW_DAYS = 20
TABLE_MAX_ROWS = 50
SECTION_TABLE_ROWS = 25


def fmt_int(value: int) -> str:
    return f"{int(value):,}".replace(",", ".")


def fmt_pct(num: int, den: int) -> str:
    if den <= 0:
        return "0,0%"
    return f"{(num / den) * 100:.1f}%".replace(".", ",")


def clean_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def load_data() -> pd.DataFrame:
    if not MODEL_FILE.exists():
        raise FileNotFoundError(
            f"Nao encontrei o snapshot necessario: {MODEL_FILE}"
        )
    df = pd.read_pickle(MODEL_FILE).copy()
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    required = [
        "Logger",
        "Pedido",
        "Agente",
        "UF",
        "Data de Entrega",
        "Tipo Datalogger",
        "Status Retorno",
    ]
    for col in required:
        if col not in out.columns:
            out[col] = ""

    for col in ["Logger", "Pedido", "Agente", "UF", "Tipo Datalogger", "Status Retorno"]:
        out[col] = clean_text(out[col])

    for col in ["UF Destino", "Cidade Destino", "Destinatario", "Ultimo_Historico"]:
        if col in out.columns:
            out[col] = out[col]
        else:
            out[col] = ""

    out["Data de Entrega"] = pd.to_datetime(out["Data de Entrega"], errors="coerce")
    out = out[out["Logger"].ne("")].copy()
    out = out[out["Data de Entrega"].notna()].copy()

    today = pd.Timestamp.now().normalize()
    cutoff = today - pd.Timedelta(days=WINDOW_DAYS)
    out = out[out["Data de Entrega"] >= cutoff].copy()

    out["Dia"] = out["Data de Entrega"].dt.normalize()
    out["_data_entrega_txt"] = out["Data de Entrega"].dt.strftime("%d/%m/%Y %H:%M:%S")
    out["_dia_txt"] = out["Dia"].dt.strftime("%d/%m/%Y")
    out["_is_today"] = out["Dia"].eq(today)
    out["_is_yesterday"] = out["Dia"].eq(today - pd.Timedelta(days=1))

    if "UF Destino" in out.columns:
        out["UF Destino"] = clean_text(out["UF Destino"]).replace("", "SEM UF")
    if "Cidade Destino" in out.columns:
        out["Cidade Destino"] = clean_text(out["Cidade Destino"]).replace("", "SEM CIDADE")
    if "Destinatario" in out.columns:
        out["Destinatario"] = clean_text(out["Destinatario"]).replace("", "SEM DESTINATARIO")

    out["Agente"] = out["Agente"].replace("", "SEM AGENTE")
    out["UF"] = out["UF"].replace("", "SEM UF")
    out["Tipo Datalogger"] = out["Tipo Datalogger"].replace("", "SEM TIPO")
    if "Status Retorno" in out.columns:
        out["Status Retorno"] = out["Status Retorno"].replace("", "SEM STATUS")

    out = out.sort_values(["Data de Entrega", "Logger"], ascending=[False, True]).reset_index(drop=True)
    return out


def _series_by_day(df: pd.DataFrame) -> pd.DataFrame:
    today = pd.Timestamp.now().normalize()
    start = today - pd.Timedelta(days=WINDOW_DAYS)
    days = pd.DataFrame({"Dia": pd.date_range(start=start, end=today, freq="D")})
    if df.empty:
        days["Loggers"] = 0
        return days
    agg = (
        df.groupby("Dia", as_index=False)["Logger"]
        .nunique()
        .rename(columns={"Logger": "Loggers"})
    )
    out = days.merge(agg, how="left", on="Dia")
    out["Loggers"] = out["Loggers"].fillna(0).astype(int)
    return out


def _daily_return_stack(df: pd.DataFrame) -> pd.DataFrame:
    today = pd.Timestamp.now().normalize()
    start = today - pd.Timedelta(days=WINDOW_DAYS)
    days = pd.DataFrame({"Dia": pd.date_range(start=start, end=today, freq="D")})
    if df.empty:
        days["Retornado"] = 0
        days["Pendente"] = 0
        days["Total"] = 0
        days["PctRetornado"] = 0.0
        return days

    base = df.copy()
    base["Dia"] = pd.to_datetime(base["Dia"], errors="coerce").dt.normalize()
    base = base[base["Dia"].notna()].copy()
    base["Status Retorno"] = base["Status Retorno"].fillna("").astype(str).str.strip()

    agg = (
        base.assign(
            Retornado=base["Status Retorno"].eq("Retornado").astype(int),
            Pendente=base["Status Retorno"].eq("Pendente de Retorno").astype(int),
        )
        .groupby("Dia", as_index=False)[["Retornado", "Pendente"]]
        .sum()
    )
    out = days.merge(agg, how="left", on="Dia")
    out[["Retornado", "Pendente"]] = out[["Retornado", "Pendente"]].fillna(0).astype(int)
    out["Total"] = out["Retornado"] + out["Pendente"]
    out["PctRetornado"] = out["Retornado"].where(out["Total"].gt(0), 0) / out["Total"].where(out["Total"].gt(0), 1) * 100
    return out


def _agent_return_stack(df: pd.DataFrame, limit: int = 12) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Agente", "Retornado", "Pendente", "Total", "PctRetornado"])

    base = df.copy()
    base["Agente"] = base["Agente"].fillna("").astype(str).str.strip()
    base.loc[base["Agente"].eq(""), "Agente"] = "SEM AGENTE"
    base["Status Retorno"] = base["Status Retorno"].fillna("").astype(str).str.strip()

    agg = (
        base.assign(
            Retornado=base["Status Retorno"].eq("Retornado").astype(int),
            Pendente=base["Status Retorno"].eq("Pendente de Retorno").astype(int),
        )
        .groupby("Agente", as_index=False)[["Retornado", "Pendente"]]
        .sum()
    )
    agg["Total"] = agg["Retornado"] + agg["Pendente"]
    agg["PctRetornado"] = agg["Retornado"].where(agg["Total"].gt(0), 0) / agg["Total"].where(agg["Total"].gt(0), 1) * 100
    agg = agg.sort_values("Total", ascending=False).head(limit)
    agg = agg.sort_values("Total", ascending=True).reset_index(drop=True)
    return agg


def _top_series(df: pd.DataFrame, col: str, label: str, limit: int = 8) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[label, "Loggers"])
    out = (
        df.groupby(col, as_index=False)["Logger"]
        .nunique()
        .rename(columns={col: label, "Logger": "Loggers"})
        .sort_values("Loggers", ascending=False)
        .head(limit)
    )
    return out


DAY_COLOR = "#6f9eff"
AGENT_COLOR = "#2f6fd6"
UF_COLOR = "#9bc7ff"


def make_bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str = "#7aa2ff",
    orientation: str = "v",
    height: int = 320,
    show_text: bool = True,
):
    if df.empty:
        fig = px.bar(title=title)
        fig.add_annotation(
            text="Sem dados para exibir",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(color="#cbd5e1", size=16),
        )
        fig.update_layout(template="plotly_dark", paper_bgcolor="#0b1020", plot_bgcolor="#0b1020")
        return fig
    chart_df = df.copy()
    if orientation == "h" and y in chart_df.columns:
        chart_df = chart_df.sort_values(x, ascending=False)
    fig = px.bar(chart_df, x=x, y=y, title=title, color_discrete_sequence=[color], orientation=orientation)
    trace_kwargs = dict(
        cliponaxis=False,
        marker_line_width=0,
    )
    if show_text:
        text_template = "%{x}" if orientation == "h" else "%{y}"
        trace_kwargs["texttemplate"] = text_template
        trace_kwargs["textposition"] = "outside"
        if orientation == "h":
            trace_kwargs["textposition"] = "inside"
            trace_kwargs["insidetextanchor"] = "end"
            trace_kwargs["textfont"] = dict(color="#000000")
    fig.update_traces(**trace_kwargs)
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0b1020",
        plot_bgcolor="#0b1020",
        margin=dict(l=20, r=20, t=52, b=48),
        height=height,
        font=dict(color="#e5eefc"),
        xaxis=dict(gridcolor="#25304a"),
        yaxis=dict(
            gridcolor="#25304a",
            categoryorder="array" if orientation == "h" else "trace",
            categoryarray=chart_df[y].tolist() if orientation == "h" else None,
            autorange="reversed" if orientation == "h" else True,
        ),
        title=dict(x=0.02, font=dict(size=15)),
        bargap=0.28,
    )
    return fig


def make_line_chart(df: pd.DataFrame):
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="Sem dados para exibir",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(color="#cbd5e1", size=16),
        )
        fig.update_layout(template="plotly_dark", paper_bgcolor="#0b1020", plot_bgcolor="#0b1020")
        return fig
    chart_df = _daily_return_stack(df).copy()
    chart_df["DiaTxt"] = chart_df["Dia"].dt.strftime("%d/%m")
    fig = go.Figure()
    fig.add_bar(
        x=chart_df["DiaTxt"],
        y=chart_df["Retornado"],
        name="Retornado ao estoque",
        marker=dict(color="#2f6fd6", line=dict(width=0)),
        text=chart_df["PctRetornado"].map(lambda v: f"{v:.0f}%"),
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="#ffffff"),
        cliponaxis=False,
        hovertemplate="<b>%{x}</b><br>Retornado: %{y}<br>% retornado: %{customdata[0]:.0f}%<extra></extra>",
        customdata=chart_df[["PctRetornado"]],
    )
    fig.add_bar(
        x=chart_df["DiaTxt"],
        y=chart_df["Pendente"],
        name="Pendente de retorno",
        marker=dict(color="#9bc7ff", line=dict(width=0)),
        hovertemplate="<b>%{x}</b><br>Pendente: %{y}<extra></extra>",
    )
    for _, row in chart_df.iterrows():
        total = int(row["Total"])
        if total > 0:
            fig.add_annotation(
                x=row["DiaTxt"],
                y=total,
                text=fmt_int(total),
                showarrow=False,
                yshift=10,
                font=dict(color="#ffffff", size=11, family="Segoe UI, Tahoma, Arial, sans-serif"),
            )
    fig.update_layout(
        barmode="stack",
        template="plotly_dark",
        paper_bgcolor="#0b1020",
        plot_bgcolor="#0b1020",
        margin=dict(l=24, r=20, t=52, b=52),
        height=380,
        font=dict(color="#e5eefc"),
        xaxis=dict(gridcolor="#25304a"),
        yaxis=dict(gridcolor="#25304a"),
        title=dict(x=0.02, font=dict(size=15)),
        bargap=0.22,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0.0,
        ),
    )
    return fig


def make_rank_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str,
    height: int = 430,
):
    if df.empty:
        fig = px.bar(title=title)
        fig.add_annotation(
            text="Sem dados para exibir",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(color="#cbd5e1", size=16),
        )
        fig.update_layout(template="plotly_dark", paper_bgcolor="#0b1020", plot_bgcolor="#0b1020")
        return fig
    chart_df = df.copy().sort_values(y, ascending=True)
    fig = px.bar(
        chart_df,
        x=y,
        y=x,
        orientation="h",
        title=title,
        color_discrete_sequence=[color],
    )
    fig.update_traces(
        cliponaxis=False,
        texttemplate="%{x}",
        textposition="outside",
        marker_line_width=0,
        marker=dict(line=dict(color="rgba(255,255,255,0.05)", width=1)),
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0b1020",
        plot_bgcolor="#0b1020",
        margin=dict(l=22, r=24, t=56, b=36),
        height=height,
        font=dict(color="#e5eefc"),
        xaxis=dict(gridcolor="#25304a", zeroline=False),
        yaxis=dict(gridcolor="#25304a", automargin=True, categoryorder="total ascending"),
        title=dict(x=0.02, font=dict(size=15)),
        showlegend=False,
        bargap=0.22,
    )
    return fig


def make_agent_return_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="Sem dados para exibir",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(color="#cbd5e1", size=16),
        )
        fig.update_layout(template="plotly_dark", paper_bgcolor="#0b1020", plot_bgcolor="#0b1020")
        return fig

    chart_df = _agent_return_stack(df, limit=12)
    fig = go.Figure()
    fig.add_bar(
        y=chart_df["Agente"],
        x=chart_df["Retornado"],
        orientation="h",
        name="Retornado ao estoque",
        marker=dict(color="#2f6fd6", line=dict(width=0)),
        text=chart_df["PctRetornado"].map(lambda v: f"{v:.0f}%"),
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="#ffffff"),
        cliponaxis=False,
        customdata=chart_df[["PctRetornado"]],
        hovertemplate="<b>%{y}</b><br>Retornado: %{x}<br>% retornado: %{customdata[0]:.0f}%<extra></extra>",
    )
    fig.add_bar(
        y=chart_df["Agente"],
        x=chart_df["Pendente"],
        orientation="h",
        name="Pendente de retorno",
        marker=dict(color="#9bc7ff", line=dict(width=0)),
        cliponaxis=False,
        hovertemplate="<b>%{y}</b><br>Pendente: %{x}<extra></extra>",
    )
    fig.update_layout(
        barmode="stack",
        template="plotly_dark",
        paper_bgcolor="#0b1020",
        plot_bgcolor="#0b1020",
        margin=dict(l=22, r=24, t=56, b=36),
        height=620,
        font=dict(color="#e5eefc"),
        xaxis=dict(gridcolor="#25304a", zeroline=False),
        yaxis=dict(
            gridcolor="#25304a",
            categoryorder="array",
            categoryarray=chart_df["Agente"].tolist(),
            autorange="reversed",
            automargin=True,
        ),
        title=dict(x=0.02, font=dict(size=15)),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0.0,
        ),
        bargap=0.25,
    )
    return fig


def fig_div(fig) -> str:
    return pio.to_html(
        fig,
        include_plotlyjs=False,
        full_html=False,
        config={"displayModeBar": False, "responsive": True},
    )


def table_html(df: pd.DataFrame, columns: list[str], max_rows: int) -> tuple[str, int]:
    if df.empty:
        empty = '<div class="empty-box">Sem registros neste recorte.</div>'
        return empty, 0
    view = df.copy()
    cols = [c for c in columns if c in view.columns]
    view = view[cols].head(max_rows).copy()
    if "Data de Entrega" in view.columns:
        view["Data de Entrega"] = pd.to_datetime(view["Data de Entrega"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M:%S").fillna("")
    if "Ultimo_Historico" in view.columns:
        view["Ultimo_Historico"] = pd.to_datetime(view["Ultimo_Historico"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M:%S").fillna("")
    html = view.to_html(index=False, escape=True, classes="data-table", border=0)
    return html, len(view)


def _csv_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    cols = [
        "Pedido",
        "Logger",
        "Tipo Datalogger",
        "Agente",
        "UF",
        "Data de Entrega",
        "Status Retorno",
        "UF Destino",
        "Cidade Destino",
        "Destinatario",
        "Ultimo_Historico",
    ]
    cols = [c for c in cols if c in out.columns]
    out = out[cols].copy()
    for col in ["Data de Entrega", "Ultimo_Historico"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%d/%m/%Y %H:%M:%S").fillna("")
    return out


def build_page(df: pd.DataFrame) -> str:
    today = pd.Timestamp.now().normalize()
    yesterday = today - pd.Timedelta(days=1)

    total_loggers = int(df["Logger"].nunique()) if not df.empty else 0
    total_pedidos = int(df["Pedido"].nunique()) if not df.empty and "Pedido" in df.columns else 0
    total_agentes = int(df["Agente"].nunique()) if not df.empty and "Agente" in df.columns else 0
    total_retorno = int(df.loc[df["Status Retorno"].eq("Retornado"), "Logger"].nunique()) if not df.empty and "Status Retorno" in df.columns else 0
    total_hoje = int(df.loc[df["Dia"].eq(today), "Logger"].nunique()) if not df.empty else 0
    total_ontem = int(df.loc[df["Dia"].eq(yesterday), "Logger"].nunique()) if not df.empty else 0

    ult_entrega = ""
    if not df.empty:
        ult_dt = pd.to_datetime(df["Data de Entrega"], errors="coerce").max()
        if pd.notna(ult_dt):
            ult_entrega = ult_dt.strftime("%d/%m/%Y %H:%M:%S")

    daily_fig = make_line_chart(df)
    top_agente_fig = make_agent_return_chart(df)
    top_uf_fig = make_bar_chart(
        _top_series(df, "UF", "UF", limit=12),
        "Loggers",
        "UF",
        "Entregas por UF",
        UF_COLOR,
        orientation="h",
        height=600,
        show_text=True,
    )

    today_df = df[df["Dia"].eq(today)].copy() if not df.empty else df.head(0).copy()
    yest_df = df[df["Dia"].eq(yesterday)].copy() if not df.empty else df.head(0).copy()
    detail_cols = [
        "Pedido",
        "Logger",
        "Tipo Datalogger",
        "Agente",
        "UF",
        "Data de Entrega",
        "Status Retorno",
        "UF Destino",
        "Cidade Destino",
        "Destinatario",
    ]

    today_table, today_count = table_html(today_df, detail_cols, SECTION_TABLE_ROWS)
    yest_table, yest_count = table_html(yest_df, detail_cols, SECTION_TABLE_ROWS)
    detail_table, detail_count = table_html(df, detail_cols, TABLE_MAX_ROWS)

    csv_frame = _csv_frame(df)
    csv_frame.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")

    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="X-UA-Compatible" content="IE=edge" />
  <title>Controle de Entregas - Loggers</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --bg: #07101d;
      --panel: rgba(14, 22, 38, 0.96);
      --panel-2: rgba(10, 16, 29, 0.96);
      --line: rgba(122,162,255,0.16);
      --line-strong: rgba(122,162,255,0.28);
      --text: #e8eefb;
      --muted: #93a2b8;
      --accent: #8fb8ff;
      --accent-2: #4f8cff;
      --accent-3: #2dd4bf;
      --warn: #f59e0b;
      --danger: #fb7185;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, Arial, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(122,162,255,0.13), transparent 26%),
        radial-gradient(circle at top right, rgba(79,140,255,0.12), transparent 22%),
        linear-gradient(180deg, #09111e 0%, #050911 100%);
      color: var(--text);
    }}
    .wrap {{ max-width: 1620px; margin: 0 auto; padding: 24px 18px 40px; }}
    .hero {{
      background:
        linear-gradient(135deg, rgba(17,26,44,0.98), rgba(10,16,29,0.98)),
        radial-gradient(circle at top right, rgba(122,162,255,0.10), transparent 30%);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 24px 24px 20px;
      box-shadow: 0 18px 44px rgba(0,0,0,0.30);
      position: relative;
      overflow: hidden;
    }}
    .hero::before {{
      content: "";
      position: absolute;
      inset: 0 auto 0 0;
      width: 5px;
      background: linear-gradient(180deg, var(--accent), var(--accent-2));
    }}
    .eyebrow {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--accent);
      font-weight: 800;
      letter-spacing: 0.10em;
      text-transform: uppercase;
      font-size: 12px;
    }}
    h1 {{
      margin: 10px 0 8px;
      font-size: 31px;
      line-height: 1.1;
    }}
    .sub {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
      max-width: 1120px;
    }}
    .pill-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 16px;
    }}
    .pill {{
      border: 1px solid rgba(148,163,184,0.22);
      border-radius: 999px;
      padding: 8px 12px;
      background: rgba(255,255,255,0.03);
      color: var(--text);
      font-size: 13px;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
      margin: 16px 0 18px;
    }}
    .kpi {{
      background:
        linear-gradient(180deg, rgba(17,26,44,0.98), rgba(11,18,32,0.98));
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px 16px 14px;
      min-height: 108px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
    }}
    .kpi .label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      margin-bottom: 10px;
    }}
    .kpi .value {{
      font-size: 30px;
      font-weight: 800;
      line-height: 1;
      margin-bottom: 8px;
    }}
    .kpi .foot {{
      font-size: 12px;
      color: var(--muted);
    }}
    .chart-stack {{
      display: flex;
      flex-direction: column;
      gap: 14px;
      margin: 14px 0 18px;
    }}
    .grid2 {{
      display: grid;
      grid-template-columns: minmax(0, 2.15fr) minmax(300px, 0.95fr);
      gap: 14px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 14px 14px 10px;
      overflow: hidden;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
    }}
    .panel-wide {{
      padding-bottom: 8px;
      border-color: var(--line-strong);
    }}
    .panel-title {{
      font-size: 15px;
      font-weight: 800;
      margin: 2px 0 12px;
      color: #f6f8ff;
    }}
    .section {{
      margin-top: 18px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
    }}
    .section h2 {{
      margin: 0 0 8px;
      font-size: 20px;
    }}
    .section p {{
      margin: 0 0 14px;
      color: var(--muted);
      font-size: 13px;
    }}
    .section-head {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid rgba(122,162,255,0.26);
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
      color: var(--text);
      text-decoration: none;
      border-radius: 12px;
      padding: 10px 14px;
      font-weight: 700;
      font-size: 13px;
    }}
    .btn:hover {{ background: rgba(255,255,255,0.08); }}
    .meta {{
      color: var(--muted);
      font-size: 13px;
    }}
    .table-wrap {{
      overflow-x: auto;
      border-radius: 14px;
      border: 1px solid rgba(148,163,184,0.16);
    }}
    table.data-table {{
      width: 100%;
      border-collapse: collapse;
      background: rgba(9,14,25,0.95);
      min-width: 1050px;
    }}
    .data-table th,
    .data-table td {{
      padding: 11px 10px;
      border-bottom: 1px solid rgba(148,163,184,0.12);
      border-right: 1px solid rgba(148,163,184,0.10);
      font-size: 12px;
      text-align: left;
      white-space: nowrap;
    }}
    .data-table th {{
      position: sticky;
      top: 0;
      background: linear-gradient(180deg, rgba(30,37,54,0.98), rgba(25,31,46,0.98));
      color: #e6efff;
      z-index: 1;
    }}
    .data-table tbody tr:nth-child(even) {{ background: rgba(255,255,255,0.015); }}
    .data-table tbody tr:hover {{ background: rgba(122,162,255,0.07); }}
    .two-col {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      margin-top: 14px;
    }}
    .empty-box {{
      padding: 18px;
      color: var(--muted);
      background: rgba(10,16,29,0.92);
      border-radius: 14px;
      border: 1px dashed rgba(148,163,184,0.24);
    }}
    .footer {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 12px;
    }}
    @media (max-width: 1280px) {{
      .kpis {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
      .grid2, .two-col {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 720px) {{
      .wrap {{ padding: 14px 10px 24px; }}
      h1 {{ font-size: 24px; }}
      .kpis {{ grid-template-columns: 1fr 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="eyebrow">Controle operacional</div>
      <h1>Controle de Entregas dos Loggers</h1>
      <div class="sub">
        Visao separada para acompanhar apenas os loggers entregues nos ultimos {WINDOW_DAYS} dias.
        A pagina destaca o que foi entregue hoje, ontem e no periodo completo, com tabela limitada na visualizacao
        e exportacao completa em CSV.
      </div>
      <div class="pill-row">
        <div class="pill"><strong>Janela:</strong> ultimos {WINDOW_DAYS} dias</div>
        <div class="pill"><strong>Ultima entrega:</strong> {escape(ult_entrega) if ult_entrega else "--"}</div>
        <div class="pill"><strong>Gerado em:</strong> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}</div>
      </div>
    </div>

    <div class="kpis">
      <div class="kpi"><div class="label">Loggers no periodo</div><div class="value">{fmt_int(total_loggers)}</div><div class="foot">Entregas com data valida</div></div>
      <div class="kpi"><div class="label">Entregues hoje</div><div class="value">{fmt_int(total_hoje)}</div><div class="foot">Data de entrega igual a hoje</div></div>
      <div class="kpi"><div class="label">Entregues ontem</div><div class="value">{fmt_int(total_ontem)}</div><div class="foot">Referencia para o turno anterior</div></div>
      <div class="kpi"><div class="label">Pedidos unicos</div><div class="value">{fmt_int(total_pedidos)}</div><div class="foot">Pedidos com logger entregue</div></div>
      <div class="kpi"><div class="label">Agentes unicos</div><div class="value">{fmt_int(total_agentes)}</div><div class="foot">Responsaveis da entrega</div></div>
      <div class="kpi"><div class="label">Retornados no periodo</div><div class="value">{fmt_int(total_retorno)}</div><div class="foot">Status Retorno = Retornado</div></div>
    </div>

    <div class="chart-stack">
      <div class="panel panel-wide">
        <div class="panel-title">Entregas por dia</div>
        {fig_div(daily_fig)}
      </div>
      <div class="grid2">
        <div class="panel">
          <div class="panel-title">Top agentes</div>
          {fig_div(top_agente_fig)}
        </div>
        <div class="panel">
          <div class="panel-title">Top UFs</div>
          {fig_div(top_uf_fig)}
        </div>
      </div>
    </div>

    <div class="section">
      <div class="section-head">
        <div>
          <h2>Entregas de hoje</h2>
          <p>{fmt_int(today_count)} registro(s) encontrados para a data atual.</p>
        </div>
      </div>
      <div class="table-wrap">{today_table}</div>
    </div>

    <div class="section">
      <div class="section-head">
        <div>
          <h2>Entregas de ontem</h2>
          <p>{fmt_int(yest_count)} registro(s) encontrados para o dia anterior.</p>
        </div>
      </div>
      <div class="table-wrap">{yest_table}</div>
    </div>

    <div class="section">
      <div class="section-head">
        <div>
          <h2>Detalhe do periodo</h2>
          <p>{fmt_int(detail_count)} linhas exibidas na tabela; o CSV completo sai em <code>CONTROLE_ENTREGAS_20D.csv</code>.</p>
        </div>
        <a class="btn" href="CONTROLE_ENTREGAS_20D.csv" download>Baixar CSV completo</a>
      </div>
      <div class="table-wrap">{detail_table}</div>
    </div>

    <div class="footer">
      Fonte: snapshot_reversa/modelo_final.pkl. O recorte considera apenas loggers com data de entrega valida e janela dos ultimos {WINDOW_DAYS} dias.
    </div>
  </div>
</body>
</html>"""
    return html


def main() -> None:
    print("[controle] Carregando snapshot consolidado...")
    raw = load_data()
    df = prepare_data(raw)
    print(f"[controle] Registros no recorte: {len(df)}")
    print(f"[controle] Loggers unicos: {df['Logger'].nunique() if not df.empty else 0}")
    print(f"[controle] Pedidos unicos: {df['Pedido'].nunique() if not df.empty else 0}")
    html = build_page(df)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"[controle] HTML salvo: {OUTPUT_HTML}")
    print(f"[controle] CSV salvo: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
