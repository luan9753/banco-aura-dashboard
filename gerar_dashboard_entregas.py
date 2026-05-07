import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import psycopg2

from env_utils import load_env_file


load_env_file()

DEFAULT_HOST = "db.lwfiljyxrlahuhjddfnp.supabase.co"
DEFAULT_DATABASE = "postgres"
DEFAULT_USER = "readonly_user"
DEFAULT_PASSWORD = os.getenv("AURA_DB_PASSWORD", "")
DEFAULT_PORT = 5432
DEFAULT_START_DATE = "2026-04-10"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera dashboard HTML com proporção de pedidos/loggers inseridos "
            "e séries diárias desde uma data."
        )
    )
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=os.getenv("AURA_END_DATE", ""))
    parser.add_argument("--output", default="")
    parser.add_argument("--host", default=os.getenv("AURA_DB_HOST", DEFAULT_HOST))
    parser.add_argument("--database", default=os.getenv("AURA_DB_NAME", DEFAULT_DATABASE))
    parser.add_argument("--user", default=os.getenv("AURA_DB_USER", DEFAULT_USER))
    parser.add_argument("--password", default=os.getenv("AURA_DB_PASSWORD", DEFAULT_PASSWORD))
    parser.add_argument("--port", type=int, default=int(os.getenv("AURA_DB_PORT", DEFAULT_PORT)))
    return parser.parse_args()


def get_connection(args: argparse.Namespace):
    return psycopg2.connect(
        host=args.host,
        database=args.database,
        user=args.user,
        password=args.password,
        port=args.port,
    )


def build_output_path(start_date: str, end_date: str | None, output: str) -> str:
    if output:
        return output
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if end_date:
        return os.path.join(
            "Banco_Aura",
            f"dashboard_entregas_{start_date}_a_{end_date}_{stamp}.html",
        )
    return os.path.join(
        "Banco_Aura",
        f"dashboard_entregas_desde_{start_date}_{stamp}.html",
    )


def _load_sqlserver_conn_string() -> str:
    env_cs = os.getenv("AURA_SQLSERVER_CONN_STRING", "").strip()
    if env_cs:
        return env_cs

    workspace_root = Path(__file__).resolve().parent.parent
    odc_candidates = [
        workspace_root / "10.141.0.111_Entregas_Dashboard.odc",
        workspace_root / "GRUAG_02_Entregas_Dashboard.odc",
    ]
    for odc_path in odc_candidates:
        if not odc_path.exists():
            continue
        txt = odc_path.read_text(encoding="utf-8", errors="ignore")
        match = re.search(
            r"DRIVER=\{ODBC Driver 18 for SQL Server\};SERVER=.*?TrustServerCertificate=yes;",
            txt,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match:
            return re.sub(r"\s+", " ", match.group(0)).strip()
    return ""


def query_delivery_launch_metrics(start_date: str, end_date: str | None = None) -> dict:
    metrics = {
        "available": False,
        "pedidos_com_entrega_e_lancamento": 0,
        "pedidos_validos": 0,
        "pedidos_negativos": 0,
        "media_horas": None,
        "media_dias": None,
        "daily": [],
        "error": "",
    }

    conn_str = _load_sqlserver_conn_string()
    if not conn_str:
        metrics["error"] = "connection_string_sqlserver_nao_encontrada"
        return metrics

    try:
        import pyodbc
    except Exception:
        metrics["error"] = "pyodbc_nao_disponivel"
        return metrics

    try:
        start_sql = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
    except ValueError:
        metrics["error"] = f"start_date_invalida:{start_date}"
        return metrics
    end_sql = None
    if end_date:
        try:
            end_sql = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
        except ValueError:
            metrics["error"] = f"end_date_invalida:{end_date}"
            return metrics

    summary_sql = """
WITH entregas AS (
    SELECT
        mov.nr_PedidoCliente AS Pedido,
        CASE
            WHEN ocn32.dt_PrazoFechamento IS NOT NULL
             AND TRY_CAST(LEFT(ocn32.hr_PrazoFechamento, 5) AS time(0)) IS NOT NULL
            THEN DATETIMEFROMPARTS(
                YEAR(ocn32.dt_PrazoFechamento),
                MONTH(ocn32.dt_PrazoFechamento),
                DAY(ocn32.dt_PrazoFechamento),
                DATEPART(hour, TRY_CAST(LEFT(ocn32.hr_PrazoFechamento, 5) AS time(0))),
                DATEPART(minute, TRY_CAST(LEFT(ocn32.hr_PrazoFechamento, 5) AS time(0))),
                0, 0
            )
            ELSE NULL
        END AS Data_Entrega,
        CASE
            WHEN ocn32.dt_Abertura IS NOT NULL
             AND TRY_CAST(LEFT(ocn32.hr_Abertura, 5) AS time(0)) IS NOT NULL
            THEN DATETIMEFROMPARTS(
                YEAR(ocn32.dt_Abertura),
                MONTH(ocn32.dt_Abertura),
                DAY(ocn32.dt_Abertura),
                DATEPART(hour, TRY_CAST(LEFT(ocn32.hr_Abertura, 5) AS time(0))),
                DATEPART(minute, TRY_CAST(LEFT(ocn32.hr_Abertura, 5) AS time(0))),
                0, 0
            )
            ELSE NULL
        END AS Data_Lancamento,
        ROW_NUMBER() OVER (
            PARTITION BY mov.nr_PedidoCliente, mov.id_Movimento
            ORDER BY ocn32.dt_Abertura DESC, ocn32.hr_Abertura DESC, ocn32.id_OcorrenciaNota DESC
        ) AS rn
    FROM dbo.tbdMovimento mov
    INNER JOIN dbo.tbdOcorrenciaNota ocn32
        ON ocn32.id_Movimento = mov.id_Movimento
       AND ocn32.id_Ocorrencia = 32
    WHERE ocn32.dt_PrazoFechamento >= CONVERT(date, ?, 112)
      AND (? IS NULL OR ocn32.dt_PrazoFechamento <= CONVERT(date, ?, 112))
),
base AS (
    SELECT
        Pedido,
        Data_Entrega,
        Data_Lancamento,
        DATEDIFF_BIG(MINUTE, Data_Entrega, Data_Lancamento) AS Minutos_Lancamento
    FROM entregas
    WHERE rn = 1
      AND Data_Entrega IS NOT NULL
      AND Data_Lancamento IS NOT NULL
),
validos AS (
    SELECT * FROM base WHERE Minutos_Lancamento >= 0
)
SELECT
    (SELECT COUNT(*) FROM base) AS pedidos_com_entrega_e_lancamento,
    (SELECT COUNT(*) FROM validos) AS pedidos_validos,
    (SELECT COUNT(*) FROM base WHERE Minutos_Lancamento < 0) AS pedidos_negativos,
    CAST(AVG(CAST(Minutos_Lancamento AS float))/60.0 AS decimal(18,2)) AS media_horas,
    CAST(AVG(CAST(Minutos_Lancamento AS float))/1440.0 AS decimal(18,2)) AS media_dias
FROM validos;
"""

    daily_sql = """
WITH entregas AS (
    SELECT
        mov.nr_PedidoCliente AS Pedido,
        CASE
            WHEN ocn32.dt_PrazoFechamento IS NOT NULL
             AND TRY_CAST(LEFT(ocn32.hr_PrazoFechamento, 5) AS time(0)) IS NOT NULL
            THEN DATETIMEFROMPARTS(
                YEAR(ocn32.dt_PrazoFechamento),
                MONTH(ocn32.dt_PrazoFechamento),
                DAY(ocn32.dt_PrazoFechamento),
                DATEPART(hour, TRY_CAST(LEFT(ocn32.hr_PrazoFechamento, 5) AS time(0))),
                DATEPART(minute, TRY_CAST(LEFT(ocn32.hr_PrazoFechamento, 5) AS time(0))),
                0, 0
            )
            ELSE NULL
        END AS Data_Entrega,
        CASE
            WHEN ocn32.dt_Abertura IS NOT NULL
             AND TRY_CAST(LEFT(ocn32.hr_Abertura, 5) AS time(0)) IS NOT NULL
            THEN DATETIMEFROMPARTS(
                YEAR(ocn32.dt_Abertura),
                MONTH(ocn32.dt_Abertura),
                DAY(ocn32.dt_Abertura),
                DATEPART(hour, TRY_CAST(LEFT(ocn32.hr_Abertura, 5) AS time(0))),
                DATEPART(minute, TRY_CAST(LEFT(ocn32.hr_Abertura, 5) AS time(0))),
                0, 0
            )
            ELSE NULL
        END AS Data_Lancamento,
        ROW_NUMBER() OVER (
            PARTITION BY mov.nr_PedidoCliente, mov.id_Movimento
            ORDER BY ocn32.dt_Abertura DESC, ocn32.hr_Abertura DESC, ocn32.id_OcorrenciaNota DESC
        ) AS rn
    FROM dbo.tbdMovimento mov
    INNER JOIN dbo.tbdOcorrenciaNota ocn32
        ON ocn32.id_Movimento = mov.id_Movimento
       AND ocn32.id_Ocorrencia = 32
    WHERE ocn32.dt_PrazoFechamento >= CONVERT(date, ?, 112)
      AND (? IS NULL OR ocn32.dt_PrazoFechamento <= CONVERT(date, ?, 112))
),
base AS (
    SELECT
        Data_Entrega,
        DATEDIFF_BIG(MINUTE, Data_Entrega, Data_Lancamento) AS Minutos_Lancamento
    FROM entregas
    WHERE rn = 1
      AND Data_Entrega IS NOT NULL
      AND Data_Lancamento IS NOT NULL
      AND DATEDIFF_BIG(MINUTE, Data_Entrega, Data_Lancamento) >= 0
)
SELECT
    CAST(Data_Entrega AS date) AS dia_entrega,
    COUNT(*) AS pedidos_validos,
    CAST(AVG(CAST(Minutos_Lancamento AS float))/60.0 AS decimal(18,2)) AS media_horas
FROM base
GROUP BY CAST(Data_Entrega AS date)
ORDER BY dia_entrega;
"""

    try:
        with pyodbc.connect(conn_str, timeout=30) as conn:
            cur = conn.cursor()
            cur.execute(summary_sql, (start_sql, end_sql, end_sql))
            s_row = cur.fetchone()
            cur.execute(daily_sql, (start_sql, end_sql, end_sql))
            d_rows = cur.fetchall()
    except Exception as exc:
        metrics["error"] = str(exc)
        return metrics

    if s_row:
        metrics["pedidos_com_entrega_e_lancamento"] = int(s_row[0] or 0)
        metrics["pedidos_validos"] = int(s_row[1] or 0)
        metrics["pedidos_negativos"] = int(s_row[2] or 0)
        metrics["media_horas"] = float(s_row[3]) if s_row[3] is not None else None
        metrics["media_dias"] = float(s_row[4]) if s_row[4] is not None else None

    daily = []
    for dia, pedidos_validos, media_horas in d_rows:
        dia_iso = dia.isoformat() if hasattr(dia, "isoformat") else str(dia)
        daily.append(
            {
                "dia": dia_iso,
                "pedidos_validos": int(pedidos_validos or 0),
                "media_horas": float(media_horas) if media_horas is not None else 0.0,
            }
        )
    metrics["daily"] = daily
    metrics["available"] = metrics["media_horas"] is not None
    return metrics


def query_data(conn, start_date: str, end_date: str | None = None):
    sql = """
WITH so AS (
  SELECT
    so.id,
    so.order_code,
    so.delivery_date,
    so.delivery_date::date AS dia
  FROM public.sync_orders so
  WHERE so.delivery_date::date >= %s::date
    AND (%s::date IS NULL OR so.delivery_date::date <= %s::date)
),
so_orders AS (
  SELECT DISTINCT
    so.order_code,
    so.delivery_date,
    so.dia
  FROM so
  WHERE so.order_code IS NOT NULL AND btrim(so.order_code) <> ''
),
order_keys_all AS (
  SELECT DISTINCT
    regexp_replace(
      upper(coalesce(o.order_code, '') || coalesce(oi.item_label, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  FROM public.orders o
  JOIN public.order_items oi ON oi.fk_order = o.id
),
daily_orders AS (
  SELECT
    so_o.dia,
    count(DISTINCT so_o.order_code) AS pedidos_entregues,
    count(DISTINCT CASE WHEN o.id IS NOT NULL THEN so_o.order_code END) AS pedidos_inseridos
  FROM so_orders so_o
  LEFT JOIN public.orders o ON o.order_code = so_o.order_code
  GROUP BY so_o.dia
),
daily_latency AS (
  SELECT
    so_o.dia,
    avg(extract(epoch FROM (o.created_at - so_o.delivery_date)) / 3600.0)
      FILTER (WHERE o.created_at IS NOT NULL AND so_o.delivery_date IS NOT NULL) AS avg_horas_pedidos
  FROM so_orders so_o
  JOIN public.orders o ON o.order_code = so_o.order_code
  GROUP BY so_o.dia
),
sensor_sync_items AS (
  SELECT
    coalesce(si.delivery_date, so.delivery_date)::date AS dia,
    coalesce(si.delivery_date, so.delivery_date) AS delivery_date_item,
    CASE
      WHEN upper(coalesce(si.device_serial, '')) LIKE 'TA%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial, '')) LIKE 'A%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial, '')) LIKE 'S%%' THEN 'SYOS'
      WHEN upper(coalesce(si.device_serial, '')) LIKE 'V%%' OR upper(coalesce(si.device_serial, '')) LIKE 'B%%' THEN 'Shield'
      ELSE 'Sensor web'
    END AS sensor,
    regexp_replace(
      upper(coalesce(so.order_code, '') || coalesce(si.device_serial, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  FROM so
  JOIN public.sync_items si ON si.sync_order_id = so.id
),
aura_item_created AS (
  SELECT
    regexp_replace(
      upper(coalesce(o.order_code, '') || coalesce(oi.item_label, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k,
    min(o.created_at) AS order_created_at
  FROM public.orders o
  JOIN public.order_items oi ON oi.fk_order = o.id
  WHERE o.created_at IS NOT NULL
  GROUP BY 1
),
daily_latency_sensor AS (
  SELECT
    ssi.dia,
    avg(extract(epoch FROM (aic.order_created_at - ssi.delivery_date_item)) / 3600.0)
      FILTER (
        WHERE ssi.sensor = 'ARES'
          AND aic.order_created_at IS NOT NULL
          AND ssi.delivery_date_item IS NOT NULL
      ) AS avg_horas_itens_ares,
    avg(extract(epoch FROM (aic.order_created_at - ssi.delivery_date_item)) / 3600.0)
      FILTER (
        WHERE ssi.sensor = 'SYOS'
          AND aic.order_created_at IS NOT NULL
          AND ssi.delivery_date_item IS NOT NULL
      ) AS avg_horas_itens_syos,
    avg(extract(epoch FROM (aic.order_created_at - ssi.delivery_date_item)) / 3600.0)
      FILTER (
        WHERE ssi.sensor = 'Shield'
          AND aic.order_created_at IS NOT NULL
          AND ssi.delivery_date_item IS NOT NULL
      ) AS avg_horas_itens_shield,
    avg(extract(epoch FROM (aic.order_created_at - ssi.delivery_date_item)) / 3600.0)
      FILTER (
        WHERE ssi.sensor = 'Sensor web'
          AND aic.order_created_at IS NOT NULL
          AND ssi.delivery_date_item IS NOT NULL
      ) AS avg_horas_itens_sensor_web
  FROM sensor_sync_items ssi
  JOIN aura_item_created aic ON aic.k = ssi.k
  WHERE ssi.k <> ''
  GROUP BY ssi.dia
),
daily_loggers AS (
  SELECT
    so.dia,
    count(*) FILTER (WHERE k.k <> '') AS loggers_entregues,
    count(*) FILTER (WHERE k.k <> '' AND ok.k IS NOT NULL) AS loggers_inseridos
  FROM so
  JOIN public.sync_items si ON si.sync_order_id = so.id
  CROSS JOIN LATERAL (
    SELECT regexp_replace(
      upper(coalesce(so.order_code, '') || coalesce(si.device_serial, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  ) k
  LEFT JOIN order_keys_all ok ON ok.k = k.k
  GROUP BY so.dia
),
daily AS (
  SELECT
    d.dia,
    coalesce(o.pedidos_entregues, 0) AS pedidos_entregues,
    coalesce(o.pedidos_inseridos, 0) AS pedidos_inseridos,
    coalesce(l.loggers_entregues, 0) AS loggers_entregues,
    coalesce(l.loggers_inseridos, 0) AS loggers_inseridos,
    lat.avg_horas_pedidos AS avg_horas_pedidos,
    lat_s.avg_horas_itens_ares AS avg_horas_itens_ares,
    lat_s.avg_horas_itens_syos AS avg_horas_itens_syos,
    lat_s.avg_horas_itens_shield AS avg_horas_itens_shield,
    lat_s.avg_horas_itens_sensor_web AS avg_horas_itens_sensor_web
  FROM (
    SELECT dia FROM daily_orders
    UNION
    SELECT dia FROM daily_loggers
  ) d
  LEFT JOIN daily_orders o ON o.dia = d.dia
  LEFT JOIN daily_loggers l ON l.dia = d.dia
  LEFT JOIN daily_latency lat ON lat.dia = d.dia
  LEFT JOIN daily_latency_sensor lat_s ON lat_s.dia = d.dia
),
totals AS (
  SELECT
    sum(pedidos_entregues) AS pedidos_entregues_total,
    sum(pedidos_inseridos) AS pedidos_inseridos_total,
    sum(loggers_entregues) AS loggers_entregues_total,
    sum(loggers_inseridos) AS loggers_inseridos_total
  FROM daily
)
SELECT
  to_char(dia, 'YYYY-MM-DD') AS dia,
  pedidos_entregues,
  pedidos_inseridos,
  loggers_entregues,
  loggers_inseridos,
  avg_horas_pedidos,
  avg_horas_itens_ares,
  avg_horas_itens_syos,
  avg_horas_itens_shield,
  avg_horas_itens_sensor_web,
  (SELECT pedidos_entregues_total FROM totals) AS pedidos_entregues_total,
  (SELECT pedidos_inseridos_total FROM totals) AS pedidos_inseridos_total,
  (SELECT loggers_entregues_total FROM totals) AS loggers_entregues_total,
  (SELECT loggers_inseridos_total FROM totals) AS loggers_inseridos_total
FROM daily
ORDER BY dia;
"""
    sensor_sql = """
WITH so AS (
  SELECT
    so.id,
    so.order_code
  FROM public.sync_orders so
  WHERE so.delivery_date::date >= %s::date
    AND (%s::date IS NULL OR so.delivery_date::date <= %s::date)
),
order_keys_all AS (
  SELECT DISTINCT
    regexp_replace(
      upper(coalesce(o.order_code, '') || coalesce(oi.item_label, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  FROM public.orders o
  JOIN public.order_items oi ON oi.fk_order = o.id
),
base AS (
  SELECT
    CASE
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'TA%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'A%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'S%%' THEN 'SYOS'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'V%%' OR upper(coalesce(si.device_serial,'')) LIKE 'B%%' THEN 'Shield'
      ELSE 'Sensor web'
    END AS sensor,
    regexp_replace(
      upper(coalesce(so.order_code, '') || coalesce(si.device_serial, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  FROM so
  JOIN public.sync_items si ON si.sync_order_id = so.id
),
pend AS (
  SELECT b.sensor
  FROM base b
  LEFT JOIN order_keys_all ok ON ok.k = b.k
  WHERE b.k <> '' AND ok.k IS NULL
)
SELECT sensor, count(*) AS pendentes
FROM pend
GROUP BY sensor;
"""
    sensor_daily_sql = """
WITH so AS (
  SELECT
    so.id,
    so.order_code,
    so.delivery_date::date AS dia
  FROM public.sync_orders so
  WHERE so.delivery_date::date >= %s::date
    AND (%s::date IS NULL OR so.delivery_date::date <= %s::date)
),
order_keys_all AS (
  SELECT DISTINCT
    regexp_replace(
      upper(coalesce(o.order_code, '') || coalesce(oi.item_label, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  FROM public.orders o
  JOIN public.order_items oi ON oi.fk_order = o.id
),
base AS (
  SELECT
    so.dia,
    CASE
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'TA%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'A%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'S%%' THEN 'SYOS'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'V%%' OR upper(coalesce(si.device_serial,'')) LIKE 'B%%' THEN 'Shield'
      ELSE 'Sensor web'
    END AS sensor,
    CASE
      WHEN coalesce(
        replace(
          substring(
            coalesce(
              si.metadata #>> '{product_info,thermal_type}',
              si.metadata ->> 'thermal_type',
              ''
            ) from '(-?[0-9]+(?:[.,][0-9]+)?)'
          ),
          ',',
          '.'
        )::numeric,
        si.expected_temp_min
      ) >= 0 THEN 'refrigerado'
      WHEN coalesce(
        replace(
          substring(
            coalesce(
              si.metadata #>> '{product_info,thermal_type}',
              si.metadata ->> 'thermal_type',
              ''
            ) from '(-?[0-9]+(?:[.,][0-9]+)?)'
          ),
          ',',
          '.'
        )::numeric,
        si.expected_temp_min
      ) < 0 THEN 'congelado'
      ELSE 'nao_classificado'
    END AS thermal_class,
    regexp_replace(
      upper(coalesce(so.order_code, '') || coalesce(si.device_serial, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  FROM so
  JOIN public.sync_items si ON si.sync_order_id = so.id
),
pend AS (
  SELECT b.dia, b.sensor
  FROM base b
  LEFT JOIN order_keys_all ok ON ok.k = b.k
  WHERE b.k <> '' AND ok.k IS NULL
)
SELECT to_char(dia, 'YYYY-MM-DD') AS dia, sensor, count(*) AS pendentes
FROM pend
GROUP BY dia, sensor
ORDER BY dia, sensor;
"""
    sensor_daily_stats_sql = """
WITH so AS (
  SELECT
    so.id,
    so.order_code,
    so.delivery_date::date AS dia
  FROM public.sync_orders so
  WHERE so.delivery_date::date >= %s::date
    AND (%s::date IS NULL OR so.delivery_date::date <= %s::date)
),
order_keys_all AS (
  SELECT DISTINCT
    regexp_replace(
      upper(coalesce(o.order_code, '') || coalesce(oi.item_label, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  FROM public.orders o
  JOIN public.order_items oi ON oi.fk_order = o.id
),
base AS (
  SELECT
    so.dia,
    CASE
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'TA%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'A%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'S%%' THEN 'SYOS'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'V%%' OR upper(coalesce(si.device_serial,'')) LIKE 'B%%' THEN 'Shield'
      ELSE 'Sensor web'
    END AS sensor,
    CASE
      WHEN coalesce(
        replace(
          substring(
            coalesce(
              si.metadata #>> '{product_info,thermal_type}',
              si.metadata ->> 'thermal_type',
              ''
            ) from '(-?[0-9]+(?:[.,][0-9]+)?)'
          ),
          ',',
          '.'
        )::numeric,
        si.expected_temp_min
      ) >= 0 THEN 'refrigerado'
      WHEN coalesce(
        replace(
          substring(
            coalesce(
              si.metadata #>> '{product_info,thermal_type}',
              si.metadata ->> 'thermal_type',
              ''
            ) from '(-?[0-9]+(?:[.,][0-9]+)?)'
          ),
          ',',
          '.'
        )::numeric,
        si.expected_temp_min
      ) < 0 THEN 'congelado'
      ELSE 'nao_classificado'
    END AS thermal_class,
    regexp_replace(
      upper(coalesce(so.order_code, '') || coalesce(si.device_serial, '')),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS k
  FROM so
  JOIN public.sync_items si ON si.sync_order_id = so.id
)
SELECT
  to_char(b.dia, 'YYYY-MM-DD') AS dia,
  b.sensor,
  b.thermal_class,
  count(*) FILTER (WHERE b.k <> '') AS loggers_entregues,
  count(*) FILTER (WHERE b.k <> '' AND ok.k IS NOT NULL) AS loggers_inseridos,
  count(*) FILTER (WHERE b.k <> '' AND ok.k IS NULL) AS loggers_pendentes
FROM base b
LEFT JOIN order_keys_all ok ON ok.k = b.k
GROUP BY b.dia, b.sensor, b.thermal_class
ORDER BY b.dia, b.sensor, b.thermal_class;
"""
    order_daily_stats_sql = """
WITH so AS (
  SELECT
    so.id,
    so.order_code,
    so.delivery_date::date AS dia
  FROM public.sync_orders so
  WHERE so.delivery_date::date >= %s::date
    AND (%s::date IS NULL OR so.delivery_date::date <= %s::date)
    AND so.order_code IS NOT NULL
    AND btrim(so.order_code) <> ''
),
base AS (
  SELECT
    so.dia,
    so.order_code,
    CASE
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'TA%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'A%%' THEN 'ARES'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'S%%' THEN 'SYOS'
      WHEN upper(coalesce(si.device_serial,'')) LIKE 'V%%' OR upper(coalesce(si.device_serial,'')) LIKE 'B%%' THEN 'Shield'
      ELSE 'Sensor web'
    END AS sensor,
    CASE
      WHEN coalesce(
        replace(
          substring(
            coalesce(
              si.metadata #>> '{product_info,thermal_type}',
              si.metadata ->> 'thermal_type',
              ''
            ) from '(-?[0-9]+(?:[.,][0-9]+)?)'
          ),
          ',',
          '.'
        )::numeric,
        si.expected_temp_min
      ) >= 0 THEN 'refrigerado'
      WHEN coalesce(
        replace(
          substring(
            coalesce(
              si.metadata #>> '{product_info,thermal_type}',
              si.metadata ->> 'thermal_type',
              ''
            ) from '(-?[0-9]+(?:[.,][0-9]+)?)'
          ),
          ',',
          '.'
        )::numeric,
        si.expected_temp_min
      ) < 0 THEN 'congelado'
      ELSE 'nao_classificado'
    END AS thermal_class
  FROM so
  JOIN public.sync_items si ON si.sync_order_id = so.id
),
agg AS (
  SELECT
    b.dia,
    b.sensor,
    b.thermal_class,
    count(DISTINCT b.order_code) AS pedidos_entregues,
    count(DISTINCT CASE WHEN o.id IS NOT NULL THEN b.order_code END) AS pedidos_inseridos
  FROM base b
  LEFT JOIN public.orders o ON o.order_code = b.order_code
  GROUP BY b.dia, b.sensor, b.thermal_class
)
SELECT
  to_char(dia, 'YYYY-MM-DD') AS dia,
  sensor,
  thermal_class,
  pedidos_entregues,
  pedidos_inseridos
FROM agg
ORDER BY dia, sensor, thermal_class;
"""
    latency_sql = """
WITH so_base AS (
  SELECT
    so.order_code,
    max(so.delivery_date) AS delivery_date
  FROM public.sync_orders so
  WHERE so.delivery_date::date >= %s::date
    AND (%s::date IS NULL OR so.delivery_date::date <= %s::date)
    AND so.order_code IS NOT NULL
    AND btrim(so.order_code) <> ''
  GROUP BY so.order_code
),
matched AS (
  SELECT
    o.order_code,
    o.created_at AS aura_created_at,
    s.delivery_date,
    extract(epoch FROM (o.created_at - s.delivery_date)) / 3600.0 AS hrs_after_delivery
  FROM public.orders o
  JOIN so_base s ON s.order_code = o.order_code
  WHERE o.created_at IS NOT NULL
    AND s.delivery_date IS NOT NULL
)
SELECT
  count(*) AS pedidos_comparados,
  count(*) FILTER (WHERE hrs_after_delivery >= 0) AS pedidos_validos,
  avg(hrs_after_delivery) FILTER (WHERE hrs_after_delivery >= 0) AS avg_horas,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY hrs_after_delivery) FILTER (WHERE hrs_after_delivery >= 0) AS p50_horas,
  count(*) FILTER (WHERE hrs_after_delivery < 0) AS pedidos_negativos
FROM matched;
"""
    with conn.cursor() as cur:
        params = (start_date, end_date, end_date)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.execute(sensor_sql, params)
        sensor_rows = cur.fetchall()
        cur.execute(sensor_daily_sql, params)
        sensor_daily_rows = cur.fetchall()
        cur.execute(sensor_daily_stats_sql, params)
        sensor_daily_stats_rows = cur.fetchall()
        cur.execute(order_daily_stats_sql, params)
        order_daily_stats_rows = cur.fetchall()
        cur.execute(latency_sql, params)
        latency_row = cur.fetchone()
    delivery_launch = query_delivery_launch_metrics(start_date, end_date)
    return (
        rows,
        sensor_rows,
        sensor_daily_rows,
        sensor_daily_stats_rows,
        order_daily_stats_rows,
        latency_row,
        delivery_launch,
    )


def build_payload(
    rows,
    start_date: str,
    end_date: str | None,
    sensor_rows,
    sensor_daily_rows,
    sensor_daily_stats_rows,
    order_daily_stats_rows,
    latency_row,
    delivery_launch: dict,
) -> dict:
    daily = []
    totals = {
        "pedidos_entregues_total": 0,
        "pedidos_inseridos_total": 0,
        "loggers_entregues_total": 0,
        "loggers_inseridos_total": 0,
    }
    for row in rows:
        (
            dia,
            p_ent,
            p_ins,
            l_ent,
            l_ins,
            avg_h_ped,
            avg_h_it_ares,
            avg_h_it_syos,
            avg_h_it_shield,
            avg_h_it_web,
            t_pe,
            t_pi,
            t_le,
            t_li,
        ) = row
        daily.append(
            {
                "dia": dia,
                "pedidos_entregues": int(p_ent or 0),
                "pedidos_inseridos": int(p_ins or 0),
                "loggers_entregues": int(l_ent or 0),
                "loggers_inseridos": int(l_ins or 0),
                "avg_horas_pedidos": float(avg_h_ped) if avg_h_ped is not None else None,
                "avg_horas_itens_ares": float(avg_h_it_ares) if avg_h_it_ares is not None else None,
                "avg_horas_itens_syos": float(avg_h_it_syos) if avg_h_it_syos is not None else None,
                "avg_horas_itens_shield": float(avg_h_it_shield) if avg_h_it_shield is not None else None,
                "avg_horas_itens_sensor_web": float(avg_h_it_web) if avg_h_it_web is not None else None,
            }
        )
        totals = {
            "pedidos_entregues_total": int(t_pe or 0),
            "pedidos_inseridos_total": int(t_pi or 0),
            "loggers_entregues_total": int(t_le or 0),
            "loggers_inseridos_total": int(t_li or 0),
        }

    p_total = totals["pedidos_entregues_total"]
    l_total = totals["loggers_entregues_total"]
    totals["pedidos_pct"] = (totals["pedidos_inseridos_total"] / p_total * 100) if p_total else 0
    totals["loggers_pct"] = (totals["loggers_inseridos_total"] / l_total * 100) if l_total else 0
    totals["pedidos_pendentes_total"] = p_total - totals["pedidos_inseridos_total"]
    totals["loggers_pendentes_total"] = l_total - totals["loggers_inseridos_total"]

    sensor_pending = {"ARES": 0, "SYOS": 0, "Shield": 0, "Sensor web": 0}
    for sensor, pend in sensor_rows:
        sensor_pending[str(sensor)] = int(pend or 0)
    sensor_pending_daily = []
    for dia, sensor, pend in sensor_daily_rows:
        sensor_pending_daily.append(
            {
                "dia": str(dia),
                "sensor": str(sensor),
                "pendentes": int(pend or 0),
            }
        )
    sensor_daily_stats = []
    for dia, sensor, thermal_class, l_ent, l_ins, l_pen in sensor_daily_stats_rows:
        sensor_daily_stats.append(
            {
                "dia": str(dia),
                "sensor": str(sensor),
                "thermal_class": str(thermal_class),
                "loggers_entregues": int(l_ent or 0),
                "loggers_inseridos": int(l_ins or 0),
                "loggers_pendentes": int(l_pen or 0),
            }
        )
    order_daily_stats = []
    for dia, sensor, thermal_class, p_ent, p_ins in order_daily_stats_rows:
        order_daily_stats.append(
            {
                "dia": str(dia),
                "sensor": str(sensor),
                "thermal_class": str(thermal_class),
                "pedidos_entregues": int(p_ent or 0),
                "pedidos_inseridos": int(p_ins or 0),
            }
        )

    pedidos_comparados, pedidos_validos, avg_horas, p50_horas, pedidos_negativos = latency_row
    avg_h = float(avg_horas or 0.0)
    p50_h = float(p50_horas or 0.0)
    latency = {
        "pedidos_comparados": int(pedidos_comparados or 0),
        "pedidos_validos": int(pedidos_validos or 0),
        "pedidos_negativos": int(pedidos_negativos or 0),
        "avg_horas": avg_h,
        "avg_dias": avg_h / 24.0,
        "p50_horas": p50_h,
        "p50_dias": p50_h / 24.0,
    }

    return {
        "start_date": start_date,
        "end_date": end_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "totals": totals,
        "daily": daily,
        "sensor_pending": sensor_pending,
        "sensor_pending_daily": sensor_pending_daily,
        "sensor_daily_stats": sensor_daily_stats,
        "order_daily_stats": order_daily_stats,
        "latency": latency,
        "delivery_launch": delivery_launch,
    }


def render_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Dashboard Aura - Entregas e Inserções</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --card: #ffffff;
      --ink: #14202b;
      --muted: #5f6d7a;
      --line: #d9e1ea;
      --ok: #1f9d7a;
      --warn: #ce4d4d;
      --a: #2563eb;
      --b: #0ea5e9;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1200px 500px at 0% -10%, #dcecff 0%, transparent 60%),
        radial-gradient(900px 400px at 100% -15%, #d7f9ef 0%, transparent 60%),
        var(--bg);
    }}
    .wrap {{
      max-width: 1200px;
      margin: 10px auto;
      padding: 0 10px 10px;
    }}
    .header {{
      background: linear-gradient(135deg, #0f2f53 0%, #134a87 55%, #1a69b0 100%);
      color: #fff;
      border-radius: 10px;
      padding: 10px 12px;
      box-shadow: 0 6px 16px rgba(13, 36, 66, .20);
    }}
    .header h1 {{
      margin: 0 0 2px;
      font-size: 1.05rem;
    }}
    .sub {{
      color: #d9ecff;
      font-size: .82rem;
    }}
    .filters {{
      margin-top: 8px;
      padding: 8px;
      display: grid;
      grid-template-columns: repeat(7, minmax(0, 1fr));
      gap: 8px;
      align-items: end;
    }}
    .fgroup {{
      display: flex;
      flex-direction: column;
      gap: 4px;
    }}
    .flabel {{
      color: var(--muted);
      font-size: .74rem;
    }}
    .finput {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 6px 8px;
      font-size: .82rem;
      color: var(--ink);
      background: #fff;
    }}
    .fbtn {{
      border: 1px solid #1b4f8a;
      background: #1a69b0;
      color: #fff;
      border-radius: 6px;
      padding: 7px 10px;
      font-size: .82rem;
      cursor: pointer;
      font-weight: 600;
    }}
    .fbtn.secondary {{
      border-color: #9fb3c7;
      background: #eef3f8;
      color: #33485c;
    }}
    .filter-state {{
      margin-top: 6px;
      color: #49627a;
      font-size: .78rem;
      font-weight: 600;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin-top: 8px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
      box-shadow: 0 2px 8px rgba(23, 35, 50, .05);
    }}
    .k {{
      color: var(--muted);
      font-size: .74rem;
      margin-bottom: 2px;
    }}
    .v {{
      font-size: 1.15rem;
      font-weight: 700;
      line-height: 1.1;
    }}
    .progress {{
      margin-top: 4px;
      height: 6px;
      border-radius: 999px;
      background: #e7edf4;
      overflow: hidden;
    }}
    .fill {{
      height: 100%;
      border-radius: 999px;
      background: linear-gradient(90deg, var(--ok), #36bb96);
      width: 0%;
    }}
    .section {{
      margin-top: 8px;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px;
      box-shadow: 0 2px 8px rgba(23, 35, 50, .05);
    }}
    .section h2 {{
      margin: 0 0 6px;
      font-size: .9rem;
    }}
    .chart-wrap {{
      width: 100%;
      overflow-x: auto;
    }}
    svg {{
      width: 100%;
      min-width: 760px;
      height: 255px;
      display: block;
      background: #fff;
      border: 1px solid #e8edf3;
      border-radius: 8px;
    }}
    .legend {{
      display: flex;
      gap: 10px;
      margin-bottom: 4px;
      color: var(--muted);
      font-size: .76rem;
    }}
    .dot {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
      margin-right: 6px;
      transform: translateY(1px);
    }}
    .t {{
      margin-top: 6px;
      color: var(--muted);
      font-size: .74rem;
    }}
    .sensor-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin-top: 6px;
    }}
    .sensor-card {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px;
      background: #fbfdff;
    }}
    .sensor-name {{
      font-size: .78rem;
      color: var(--muted);
      margin-bottom: 2px;
    }}
    .sensor-val {{
      font-size: 1.05rem;
      font-weight: 700;
      color: #1f2e3b;
      line-height: 1.1;
    }}
    .latency-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 280px));
      gap: 8px;
      margin-top: 6px;
    }}
    @media (max-width: 950px) {{
      .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .sensor-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .latency-grid {{ grid-template-columns: repeat(1, minmax(0, 1fr)); }}
      .filters {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <h1>Dashboard simples de entregas x inserções (Aura)</h1>
      <div class="sub" id="subtitle"></div>
    </div>
    <div class="section filters">
      <div class="fgroup">
        <div class="flabel">Data inicial (entrega)</div>
        <input class="finput" type="date" id="flt_start" />
      </div>
      <div class="fgroup">
        <div class="flabel">Data final (entrega)</div>
        <input class="finput" type="date" id="flt_end" />
      </div>
      <div class="fgroup">
        <div class="flabel">Sensor (gráficos por sensor)</div>
        <select class="finput" id="flt_sensor">
          <option value="all">Todos</option>
          <option value="ares">ARES</option>
          <option value="syos">SYOS</option>
          <option value="shield">Shield</option>
          <option value="web">Sensor web</option>
        </select>
      </div>
      <div class="fgroup">
        <div class="flabel">Faixa térmica</div>
        <select class="finput" id="flt_thermal">
          <option value="all">Todos</option>
          <option value="refrigerado">Refrigerado (>= 0°C)</option>
          <option value="congelado">Congelado (< 0°C)</option>
        </select>
      </div>
      <div class="fgroup">
        <div class="flabel">Ações</div>
        <button class="fbtn" id="btn_apply" type="button">Aplicar filtro</button>
      </div>
      <div class="fgroup">
        <div class="flabel">Ações</div>
        <button class="fbtn secondary" id="btn_reset" type="button">Limpar</button>
      </div>
      <div class="fgroup">
        <div class="flabel">Atalho</div>
        <button class="fbtn secondary" id="btn_today" type="button">Somente hoje</button>
      </div>
    </div>
    <div class="filter-state" id="filter_state"></div>

    <div class="grid">
      <div class="card">
        <div class="k">Pedidos entregues</div>
        <div class="v" id="p_ent">-</div>
      </div>
      <div class="card">
        <div class="k">Pedidos inseridos</div>
        <div class="v" id="p_ins">-</div>
      </div>
      <div class="card">
        <div class="k">Loggers entregues</div>
        <div class="v" id="l_ent">-</div>
      </div>
      <div class="card">
        <div class="k">Loggers inseridos</div>
        <div class="v" id="l_ins">-</div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="k">Proporção de pedidos inseridos</div>
        <div class="v" id="p_pct">-</div>
        <div class="progress"><div class="fill" id="p_fill"></div></div>
      </div>
      <div class="card">
        <div class="k">Pedidos pendentes</div>
        <div class="v" id="p_pen">-</div>
      </div>
      <div class="card">
        <div class="k">Proporção de loggers inseridos</div>
        <div class="v" id="l_pct">-</div>
        <div class="progress"><div class="fill" id="l_fill"></div></div>
      </div>
      <div class="card">
        <div class="k">Loggers pendentes</div>
        <div class="v" id="l_pen">-</div>
      </div>
    </div>

    <div class="section">
      <h2>Pedidos por dia: entregues x inseridos</h2>
      <div class="legend">
        <div><span class="dot" style="background:#2563eb"></span>Entregues</div>
        <div><span class="dot" style="background:#0ea5e9"></span>Inseridos</div>
      </div>
      <div class="chart-wrap"><svg id="chartPedidos"></svg></div>
    </div>

    <div class="section">
      <h2 id="h2_loggers">Loggers por dia: entregues x inseridos</h2>
      <div class="legend">
        <div><span class="dot" style="background:#1f9d7a"></span>Entregues</div>
        <div><span class="dot" style="background:#65c7ae"></span>Inseridos</div>
      </div>
      <div class="chart-wrap"><svg id="chartLoggers"></svg></div>
      <div class="t">Gerado em <span id="gen_at"></span></div>
    </div>


    <div class="section">
      <h2>Loggers pendentes por tipo</h2>
      <div class="sensor-grid">
        <div class="sensor-card">
          <div class="sensor-name">ARES</div>
          <div class="sensor-val" id="pend_ares">-</div>
        </div>
        <div class="sensor-card">
          <div class="sensor-name">SYOS</div>
          <div class="sensor-val" id="pend_syos">-</div>
        </div>
        <div class="sensor-card">
          <div class="sensor-name">Shield</div>
          <div class="sensor-val" id="pend_shield">-</div>
        </div>
        <div class="sensor-card">
          <div class="sensor-name">Sensor web</div>
          <div class="sensor-val" id="pend_web">-</div>
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Tempo para inserir pedido após entrega</h2>
      <div class="latency-grid">
        <div class="sensor-card">
          <div class="sensor-name">Média (horas)</div>
          <div class="sensor-val" id="lat_avg_h">-</div>
        </div>
      </div>
      <div class="t">Cálculo: <code>orders.created_at - sync_orders.delivery_date</code> por pedido.</div>
    </div>
    <div class="section">
      <h2>Tempo para lançar data de entrega no sistema</h2>
      <div class="latency-grid">
        <div class="sensor-card">
          <div class="sensor-name">Média (horas)</div>
          <div class="sensor-val" id="dl_avg_h">-</div>
        </div>
        <div class="sensor-card">
          <div class="sensor-name">Pedidos válidos</div>
          <div class="sensor-val" id="dl_valid">-</div>
        </div>
      </div>
      <div class="t">Cálculo: <code>Data_Lancamento - Data_Entrega</code> na ocorrência 32 (somente valores não negativos).</div>
      <div class="t" id="dl_status"></div>
    </div>
    <div class="section">
      <h2>Média de horas para lançar data de entrega por dia</h2>
      <div class="legend">
        <div><span class="dot" style="background:#1e40af"></span>Média horas para lançamento da entrega</div>
      </div>
      <div class="chart-wrap"><svg id="chartDeliveryLaunch"></svg></div>
      <div class="t">Exemplo: pedidos entregues em 10/04 mostram a média de horas até a data de entrega ser lançada no sistema.</div>
    </div>
    <div class="section">
      <h2>Média de horas por dia para inserir pedidos</h2>
      <div class="legend">
        <div><span class="dot" style="background:#2563eb"></span>Média horas (delivery_date x created_at)</div>
      </div>
      <div class="chart-wrap"><svg id="chartLatency"></svg></div>
      <div class="t">Exemplo: para 16/04, o valor mostra a média de horas dos pedidos com entrega em 16/04 que já subiram no Aura.</div>
    </div>
    <div class="section" id="secLatencyAres">
      <h2>Média de horas por dia para inserir itens ARES</h2>
      <div class="legend">
        <div><span class="dot" style="background:#1d4ed8"></span>Média horas ARES (delivery do item x created_at do pedido)</div>
      </div>
      <div class="chart-wrap"><svg id="chartLatencyAres"></svg></div>
      <div class="t">Base item a item: cruza chave de <code>sync_items</code> com <code>order_items</code>.</div>
    </div>
    <div class="section" id="secLatencySyos">
      <h2>Média de horas por dia para inserir itens SYOS</h2>
      <div class="legend">
        <div><span class="dot" style="background:#0f766e"></span>Média horas SYOS (delivery do item x created_at do pedido)</div>
      </div>
      <div class="chart-wrap"><svg id="chartLatencySyos"></svg></div>
      <div class="t">Base item a item: cruza chave de <code>sync_items</code> com <code>order_items</code>.</div>
    </div>
    <div class="section" id="secLatencyShield">
      <h2>Média de horas por dia para inserir itens Shield</h2>
      <div class="legend">
        <div><span class="dot" style="background:#b45309"></span>Média horas Shield (delivery do item x created_at do pedido)</div>
      </div>
      <div class="chart-wrap"><svg id="chartLatencyShield"></svg></div>
      <div class="t">Base item a item: cruza chave de <code>sync_items</code> com <code>order_items</code>.</div>
    </div>
    <div class="section" id="secLatencyWeb">
      <h2>Média de horas por dia para inserir itens Sensor web</h2>
      <div class="legend">
        <div><span class="dot" style="background:#475569"></span>Média horas Sensor web (delivery do item x created_at do pedido)</div>
      </div>
      <div class="chart-wrap"><svg id="chartLatencyWeb"></svg></div>
      <div class="t">Base item a item: cruza chave de <code>sync_items</code> com <code>order_items</code>.</div>
    </div>
  </div>

  <script>
    const payload = {data_json};
    const fmt = (n) => new Intl.NumberFormat('pt-BR').format(n);
    const fmtPct = (n) => `${{n.toFixed(1)}}%`;

    const periodText = payload.end_date
      ? `${{payload.start_date}} até ${{payload.end_date}}`
      : `desde ${{payload.start_date}}`;
    document.getElementById('subtitle').textContent =
      `Baseado em data de entrega ${{periodText}} | Atualizado em ${{payload.generated_at}}`;
    document.getElementById('gen_at').textContent = payload.generated_at;
    const allDaily = payload.daily || [];
    const allDlDaily = (payload.delivery_launch && payload.delivery_launch.daily) ? payload.delivery_launch.daily : [];
    const allSensorPendingDaily = payload.sensor_pending_daily || [];
    const allSensorDailyStats = payload.sensor_daily_stats || [];
    const allOrderDailyStats = payload.order_daily_stats || [];

    function drawGroupedBars(svgId, labels, aVals, bVals, colors) {{
      const svg = document.getElementById(svgId);
      const width = 1080;
      const height = 260;
      const pad = {{ top: 20, right: 12, bottom: 44, left: 46 }};
      const cw = width - pad.left - pad.right;
      const ch = height - pad.top - pad.bottom;
      svg.setAttribute('viewBox', `0 0 ${{width}} ${{height}}`);
      svg.innerHTML = '';
      if (!labels || labels.length === 0) {{
        const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        t.setAttribute('x', width / 2);
        t.setAttribute('y', height / 2);
        t.setAttribute('text-anchor', 'middle');
        t.setAttribute('font-size', '14');
        t.setAttribute('fill', '#5d7083');
        t.textContent = 'Sem dados no período';
        svg.appendChild(t);
        return;
      }}

      const maxVal = Math.max(1, ...aVals, ...bVals);
      const groupW = cw / labels.length;
      const barW = Math.max(6, Math.min(20, groupW * 0.26));
      const xAxisY = pad.top + ch;

      const axis = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      axis.setAttribute('x1', pad.left);
      axis.setAttribute('y1', xAxisY);
      axis.setAttribute('x2', pad.left + cw);
      axis.setAttribute('y2', xAxisY);
      axis.setAttribute('stroke', '#91a1b2');
      axis.setAttribute('stroke-width', '1');
      svg.appendChild(axis);

      for (let i = 0; i < 5; i++) {{
        const y = pad.top + (ch * i / 4);
        const v = Math.round(maxVal * (1 - i / 4));
        const grid = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        grid.setAttribute('x1', pad.left);
        grid.setAttribute('y1', y);
        grid.setAttribute('x2', pad.left + cw);
        grid.setAttribute('y2', y);
        grid.setAttribute('stroke', '#e6edf5');
        grid.setAttribute('stroke-width', '1');
        svg.appendChild(grid);

        const txt = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        txt.setAttribute('x', pad.left - 8);
        txt.setAttribute('y', y + 4);
        txt.setAttribute('text-anchor', 'end');
        txt.setAttribute('font-size', '11');
        txt.setAttribute('fill', '#607284');
        txt.textContent = new Intl.NumberFormat('pt-BR').format(v);
        svg.appendChild(txt);
      }}

      labels.forEach((lb, i) => {{
        const gx = pad.left + i * groupW + groupW * 0.5;
        const a = aVals[i];
        const b = bVals[i];
        const ah = (a / maxVal) * ch;
        const bh = (b / maxVal) * ch;

        const ra = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        ra.setAttribute('x', gx - barW - 2);
        ra.setAttribute('y', xAxisY - ah);
        ra.setAttribute('width', barW);
        ra.setAttribute('height', ah);
        ra.setAttribute('rx', '3');
        ra.setAttribute('fill', colors[0]);
        svg.appendChild(ra);
        if (a > 0) {{
          const la = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          la.setAttribute('x', gx - barW/2 - 2);
          la.setAttribute('y', Math.max(12, xAxisY - ah - 4));
          la.setAttribute('text-anchor', 'middle');
          la.setAttribute('font-size', '12');
          la.setAttribute('font-weight', '600');
          la.setAttribute('fill', '#2f3e4d');
          la.textContent = new Intl.NumberFormat('pt-BR').format(a);
          svg.appendChild(la);
        }}

        const rb = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        rb.setAttribute('x', gx + 2);
        rb.setAttribute('y', xAxisY - bh);
        rb.setAttribute('width', barW);
        rb.setAttribute('height', bh);
        rb.setAttribute('rx', '3');
        rb.setAttribute('fill', colors[1]);
        svg.appendChild(rb);
        if (b > 0) {{
          const lbv = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          lbv.setAttribute('x', gx + barW/2 + 2);
          lbv.setAttribute('y', Math.max(12, xAxisY - bh - 4));
          lbv.setAttribute('text-anchor', 'middle');
          lbv.setAttribute('font-size', '12');
          lbv.setAttribute('font-weight', '600');
          lbv.setAttribute('fill', '#2f3e4d');
          lbv.textContent = new Intl.NumberFormat('pt-BR').format(b);
          svg.appendChild(lbv);
        }}

        const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        t.setAttribute('x', gx);
        t.setAttribute('y', xAxisY + 14);
        t.setAttribute('text-anchor', 'middle');
        t.setAttribute('font-size', '12');
        t.setAttribute('fill', '#5d7083');
        const [y, m, d] = lb.split('-');
        t.textContent = `${{d}}/${{m}}`;
        svg.appendChild(t);
      }});
    }}

    function drawSingleBars(svgId, labels, vals, color) {{
      const svg = document.getElementById(svgId);
      const width = 1080;
      const height = 260;
      const pad = {{ top: 20, right: 12, bottom: 44, left: 52 }};
      const cw = width - pad.left - pad.right;
      const ch = height - pad.top - pad.bottom;
      svg.setAttribute('viewBox', `0 0 ${{width}} ${{height}}`);
      svg.innerHTML = '';
      if (!labels || labels.length === 0) {{
        const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        t.setAttribute('x', width / 2);
        t.setAttribute('y', height / 2);
        t.setAttribute('text-anchor', 'middle');
        t.setAttribute('font-size', '14');
        t.setAttribute('fill', '#5d7083');
        t.textContent = 'Sem dados no período';
        svg.appendChild(t);
        return;
      }}

      const maxVal = Math.max(1, ...vals);
      const groupW = cw / labels.length;
      const barW = Math.max(10, Math.min(26, groupW * 0.45));
      const xAxisY = pad.top + ch;

      const axis = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      axis.setAttribute('x1', pad.left);
      axis.setAttribute('y1', xAxisY);
      axis.setAttribute('x2', pad.left + cw);
      axis.setAttribute('y2', xAxisY);
      axis.setAttribute('stroke', '#91a1b2');
      axis.setAttribute('stroke-width', '1');
      svg.appendChild(axis);

      for (let i = 0; i < 5; i++) {{
        const y = pad.top + (ch * i / 4);
        const v = maxVal * (1 - i / 4);
        const grid = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        grid.setAttribute('x1', pad.left);
        grid.setAttribute('y1', y);
        grid.setAttribute('x2', pad.left + cw);
        grid.setAttribute('y2', y);
        grid.setAttribute('stroke', '#e6edf5');
        grid.setAttribute('stroke-width', '1');
        svg.appendChild(grid);

        const txt = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        txt.setAttribute('x', pad.left - 8);
        txt.setAttribute('y', y + 4);
        txt.setAttribute('text-anchor', 'end');
        txt.setAttribute('font-size', '11');
        txt.setAttribute('fill', '#607284');
        txt.textContent = new Intl.NumberFormat('pt-BR', {{ maximumFractionDigits: 1 }}).format(v);
        svg.appendChild(txt);
      }}

      labels.forEach((lb, i) => {{
        const gx = pad.left + i * groupW + groupW * 0.5;
        const v = vals[i];
        const h = (v / maxVal) * ch;

        const r = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        r.setAttribute('x', gx - barW/2);
        r.setAttribute('y', xAxisY - h);
        r.setAttribute('width', barW);
        r.setAttribute('height', h);
        r.setAttribute('rx', '3');
        r.setAttribute('fill', color);
        svg.appendChild(r);

        if (v > 0) {{
          const lv = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          lv.setAttribute('x', gx);
          lv.setAttribute('y', Math.max(12, xAxisY - h - 4));
          lv.setAttribute('text-anchor', 'middle');
          lv.setAttribute('font-size', '12');
          lv.setAttribute('font-weight', '600');
          lv.setAttribute('fill', '#2f3e4d');
          lv.textContent = new Intl.NumberFormat('pt-BR', {{ maximumFractionDigits: 1 }}).format(v);
          svg.appendChild(lv);
        }}

        const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        t.setAttribute('x', gx);
        t.setAttribute('y', xAxisY + 14);
        t.setAttribute('text-anchor', 'middle');
        t.setAttribute('font-size', '12');
        t.setAttribute('fill', '#5d7083');
        const [y, m, d] = lb.split('-');
        t.textContent = `${{d}}/${{m}}`;
        svg.appendChild(t);
      }});
    }}

    const dateUniverse = [
      ...new Set([
        ...allDaily.map(x => x.dia),
        ...allDlDaily.map(x => x.dia),
        ...allSensorPendingDaily.map(x => x.dia),
        ...allSensorDailyStats.map(x => x.dia),
        ...allOrderDailyStats.map(x => x.dia),
      ]),
    ].sort();
    const minDate = dateUniverse.length ? dateUniverse[0] : '';
    const maxDate = dateUniverse.length ? dateUniverse[dateUniverse.length - 1] : '';

    const fltStart = document.getElementById('flt_start');
    const fltEnd = document.getElementById('flt_end');
    const fltSensor = document.getElementById('flt_sensor');
    const fltThermal = document.getElementById('flt_thermal');
    const btnApply = document.getElementById('btn_apply');
    const btnReset = document.getElementById('btn_reset');
    const btnToday = document.getElementById('btn_today');
    const filterState = document.getElementById('filter_state');
    const sensorLabelMap = {{ all: 'Todos', ares: 'ARES', syos: 'SYOS', shield: 'Shield', web: 'Sensor web' }};
    const thermalLabelMap = {{ all: 'Todos', refrigerado: 'Refrigerado', congelado: 'Congelado' }};

    fltStart.min = minDate;
    fltStart.max = maxDate;
    fltEnd.min = minDate;
    fltEnd.max = maxDate;
    fltStart.value = minDate;
    fltEnd.value = maxDate;

    function toIsoDate(value) {{
      if (!value) return '';
      const v = String(value).trim();
      if (/^\\d{{4}}-\\d{{2}}-\\d{{2}}$/.test(v)) return v;
      const m = v.match(/^(\\d{{2}})\\/(\\d{{2}})\\/(\\d{{4}})$/);
      if (m) return `${{m[3]}}-${{m[2]}}-${{m[1]}}`;
      return '';
    }}

    function formatDateBr(iso) {{
      if (!iso) return '-';
      const m = String(iso).match(/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})$/);
      if (!m) return iso;
      return `${{m[3]}}/${{m[2]}}/${{m[1]}}`;
    }}

    function inRange(dia, start, end) {{
      if (start && dia < start) return false;
      if (end && dia > end) return false;
      return true;
    }}

    function sumBy(items, key) {{
      return items.reduce((acc, item) => acc + (Number(item[key]) || 0), 0);
    }}

    function weightedAvg(items, valKey, weightKey) {{
      let num = 0;
      let den = 0;
      for (const item of items) {{
        const v = item[valKey];
        const w = Number(item[weightKey]) || 0;
        if (v == null || w <= 0) continue;
        num += Number(v) * w;
        den += w;
      }}
      return den > 0 ? (num / den) : null;
    }}

    function setSensorSections(mode) {{
      const map = {{
        ares: document.getElementById('secLatencyAres'),
        syos: document.getElementById('secLatencySyos'),
        shield: document.getElementById('secLatencyShield'),
        web: document.getElementById('secLatencyWeb'),
      }};
      for (const [k, el] of Object.entries(map)) {{
        if (!el) continue;
        el.style.display = (mode === 'all' || mode === k) ? '' : 'none';
      }}
    }}

    function sensorNameToKey(name) {{
      const v = String(name || '').trim().toUpperCase();
      if (v === 'ARES') return 'ares';
      if (v === 'SYOS') return 'syos';
      if (v === 'SHIELD') return 'shield';
      if (v === 'SENSOR WEB') return 'web';
      return 'all';
    }}

    function thermalNameToKey(name) {{
      const v = String(name || '').trim().toLowerCase();
      if (v === 'refrigerado') return 'refrigerado';
      if (v === 'congelado') return 'congelado';
      return 'all';
    }}

    function applyFilters() {{
      let start = toIsoDate(fltStart.value) || minDate;
      let end = toIsoDate(fltEnd.value) || maxDate;
      const sensorMode = fltSensor.value || 'all';
      const thermalMode = fltThermal.value || 'all';
      if (start && end && start > end) {{
        const tmp = start;
        start = end;
        end = tmp;
        fltStart.value = start;
        fltEnd.value = end;
      }}

      const daily = allDaily.filter(x => inRange(x.dia, start, end));
      const dlDaily = allDlDaily.filter(x => inRange(x.dia, start, end));
      const sensorStats = allSensorDailyStats.filter(
        x =>
          inRange(x.dia, start, end) &&
          (sensorMode === 'all' || sensorNameToKey(x.sensor) === sensorMode) &&
          (thermalMode === 'all' || thermalNameToKey(x.thermal_class) === thermalMode)
      );
      const orderStats = allOrderDailyStats.filter(
        x =>
          inRange(x.dia, start, end) &&
          (sensorMode === 'all' || sensorNameToKey(x.sensor) === sensorMode) &&
          (thermalMode === 'all' || thermalNameToKey(x.thermal_class) === thermalMode)
      );

      const orderByDay = new Map();
      for (const row of orderStats) {{
        const k = row.dia;
        if (!orderByDay.has(k)) {{
          orderByDay.set(k, {{ ent: 0, ins: 0 }});
        }}
        const agg = orderByDay.get(k);
        agg.ent += Number(row.pedidos_entregues) || 0;
        agg.ins += Number(row.pedidos_inseridos) || 0;
      }}
      // Mantém o consolidado original quando nenhum filtro específico de logger/faixa está ativo.
      if (sensorMode === 'all' && thermalMode === 'all') {{
        orderByDay.clear();
        for (const row of daily) {{
          orderByDay.set(row.dia, {{
            ent: Number(row.pedidos_entregues) || 0,
            ins: Number(row.pedidos_inseridos) || 0,
          }});
        }}
      }}
      let pedidos_entregues_total = 0;
      let pedidos_inseridos_total = 0;
      for (const v of orderByDay.values()) {{
        pedidos_entregues_total += v.ent;
        pedidos_inseridos_total += v.ins;
      }}

      const loggerByDay = new Map();
      for (const row of sensorStats) {{
        const k = row.dia;
        if (!loggerByDay.has(k)) {{
          loggerByDay.set(k, {{ ent: 0, ins: 0, pen: 0 }});
        }}
        const agg = loggerByDay.get(k);
        agg.ent += Number(row.loggers_entregues) || 0;
        agg.ins += Number(row.loggers_inseridos) || 0;
        agg.pen += Number(row.loggers_pendentes) || 0;
      }}
      let loggers_entregues_total = 0;
      let loggers_inseridos_total = 0;
      let loggers_pendentes_total = 0;
      for (const v of loggerByDay.values()) {{
        loggers_entregues_total += v.ent;
        loggers_inseridos_total += v.ins;
        loggers_pendentes_total += v.pen;
      }}
      const pedidos_pendentes_total = pedidos_entregues_total - pedidos_inseridos_total;
      const pedidos_pct = pedidos_entregues_total > 0 ? (pedidos_inseridos_total / pedidos_entregues_total) * 100 : 0;
      const loggers_pct = loggers_entregues_total > 0 ? (loggers_inseridos_total / loggers_entregues_total) * 100 : 0;

      document.getElementById('p_ent').textContent = fmt(pedidos_entregues_total);
      document.getElementById('p_ins').textContent = fmt(pedidos_inseridos_total);
      document.getElementById('l_ent').textContent = fmt(loggers_entregues_total);
      document.getElementById('l_ins').textContent = fmt(loggers_inseridos_total);
      document.getElementById('p_pen').textContent = fmt(Math.max(0, pedidos_pendentes_total));
      document.getElementById('l_pen').textContent = fmt(Math.max(0, loggers_pendentes_total));
      document.getElementById('p_pct').textContent = fmtPct(pedidos_pct);
      document.getElementById('l_pct').textContent = fmtPct(loggers_pct);
      document.getElementById('p_fill').style.width = `${{Math.max(0, Math.min(100, pedidos_pct))}}%`;
      document.getElementById('l_fill').style.width = `${{Math.max(0, Math.min(100, loggers_pct))}}%`;

      const pendBySensor = {{ 'ARES': 0, 'SYOS': 0, 'Shield': 0, 'Sensor web': 0 }};
      for (const row of sensorStats) {{
        if (!pendBySensor.hasOwnProperty(row.sensor)) continue;
        pendBySensor[row.sensor] += Number(row.loggers_pendentes) || 0;
      }}
      document.getElementById('pend_ares').textContent = fmt(pendBySensor['ARES']);
      document.getElementById('pend_syos').textContent = fmt(pendBySensor['SYOS']);
      document.getElementById('pend_shield').textContent = fmt(pendBySensor['Shield']);
      document.getElementById('pend_web').textContent = fmt(pendBySensor['Sensor web']);

      const avgPedidoInsert = weightedAvg(daily, 'avg_horas_pedidos', 'pedidos_inseridos');
      document.getElementById('lat_avg_h').textContent = avgPedidoInsert == null ? 'N/D' : avgPedidoInsert.toFixed(1);

      const avgDl = weightedAvg(dlDaily, 'media_horas', 'pedidos_validos');
      const dlValid = sumBy(dlDaily, 'pedidos_validos');
      document.getElementById('dl_avg_h').textContent = avgDl == null ? 'N/D' : avgDl.toFixed(2);
      document.getElementById('dl_valid').textContent = dlDaily.length ? fmt(dlValid) : 'N/D';
      const dlStatus = document.getElementById('dl_status');
      if (dlStatus) {{
        if (!payload.delivery_launch || !payload.delivery_launch.available) {{
          const err = (payload.delivery_launch && payload.delivery_launch.error) ? String(payload.delivery_launch.error) : '';
          dlStatus.textContent = err
            ? `Sem dados do SQL Server: ${{err.slice(0, 180)}}`
            : 'Sem dados do SQL Server para este card.';
        }} else if (dlDaily.length === 0) {{
          dlStatus.textContent = 'Sem dados desse indicador no período selecionado.';
        }} else {{
          dlStatus.textContent = '';
        }}
      }}

      const labels = [...new Set([
        ...Array.from(orderByDay.keys()),
        ...Array.from(loggerByDay.keys()),
      ])].sort();
      const loggerLabels = labels;
      const pedidoEntVals = labels.map(d => (orderByDay.get(d)?.ent || 0));
      const pedidoInsVals = labels.map(d => (orderByDay.get(d)?.ins || 0));
      const loggerEntVals = loggerLabels.map(d => (loggerByDay.get(d)?.ent || 0));
      const loggerInsVals = loggerLabels.map(d => (loggerByDay.get(d)?.ins || 0));
      const h2Loggers = document.getElementById('h2_loggers');
      if (h2Loggers) {{
        const sensorText = sensorLabelMap[sensorMode] || sensorMode;
        const thermalText = thermalLabelMap[thermalMode] || thermalMode;
        h2Loggers.textContent = `Loggers por dia (${{sensorText}} | ${{thermalText}}): entregues x inseridos`;
      }}
      drawGroupedBars(
        'chartPedidos',
        labels,
        pedidoEntVals,
        pedidoInsVals,
        ['#2563eb', '#0ea5e9']
      );
      drawGroupedBars(
        'chartLoggers',
        loggerLabels,
        loggerEntVals,
        loggerInsVals,
        ['#1f9d7a', '#65c7ae']
      );

      const dlLabels = dlDaily.map(x => x.dia);
      drawSingleBars(
        'chartDeliveryLaunch',
        dlLabels,
        dlDaily.map(x => x.media_horas == null ? 0 : x.media_horas),
        '#1e40af'
      );
      const dailyLabels = daily.map(x => x.dia);
      drawSingleBars(
        'chartLatency',
        dailyLabels,
        daily.map(x => x.avg_horas_pedidos == null ? 0 : x.avg_horas_pedidos),
        '#2563eb'
      );
      drawSingleBars(
        'chartLatencyAres',
        dailyLabels,
        daily.map(x => x.avg_horas_itens_ares == null ? 0 : x.avg_horas_itens_ares),
        '#1d4ed8'
      );
      drawSingleBars(
        'chartLatencySyos',
        dailyLabels,
        daily.map(x => x.avg_horas_itens_syos == null ? 0 : x.avg_horas_itens_syos),
        '#0f766e'
      );
      drawSingleBars(
        'chartLatencyShield',
        dailyLabels,
        daily.map(x => x.avg_horas_itens_shield == null ? 0 : x.avg_horas_itens_shield),
        '#b45309'
      );
      drawSingleBars(
        'chartLatencyWeb',
        dailyLabels,
        daily.map(x => x.avg_horas_itens_sensor_web == null ? 0 : x.avg_horas_itens_sensor_web),
        '#475569'
      );

      setSensorSections(sensorMode);
      if (filterState) {{
        const sensorLabel = sensorLabelMap[sensorMode] || 'Todos';
        const thermalLabel = thermalLabelMap[thermalMode] || 'Todos';
        filterState.textContent = `Filtro ativo: ${{formatDateBr(start)}} a ${{formatDateBr(end)}} | Sensor: ${{sensorLabel}} | Faixa térmica: ${{thermalLabel}} | Dias com dados: ${{labels.length}}`;
      }}
    }}

    btnApply.addEventListener('click', applyFilters);
    btnReset.addEventListener('click', () => {{
      fltStart.value = minDate;
      fltEnd.value = maxDate;
      fltSensor.value = 'all';
      fltThermal.value = 'all';
      applyFilters();
    }});
    btnToday.addEventListener('click', () => {{
      const today = (payload.generated_at || '').slice(0, 10);
      if (today) {{
        fltStart.value = today;
        fltEnd.value = today;
      }}
      applyFilters();
    }});
    fltStart.addEventListener('change', applyFilters);
    fltEnd.addEventListener('change', applyFilters);
    fltSensor.addEventListener('change', applyFilters);
    fltThermal.addEventListener('change', applyFilters);

    applyFilters();
  </script>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    start_date = (args.start_date or "").strip()
    end_date = (args.end_date or "").strip() or None
    out_path = build_output_path(start_date, end_date, args.output)
    try:
        with get_connection(args) as conn:
            (
                rows,
                sensor_rows,
                sensor_daily_rows,
                sensor_daily_stats_rows,
                order_daily_stats_rows,
                latency_row,
                delivery_launch,
            ) = query_data(conn, start_date, end_date)
        payload = build_payload(
            rows,
            start_date,
            end_date,
            sensor_rows,
            sensor_daily_rows,
            sensor_daily_stats_rows,
            order_daily_stats_rows,
            latency_row,
            delivery_launch,
        )
        html = render_html(payload)
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"arquivo={out_path}")
        print(f"dias={len(payload['daily'])}")
        print(f"pedidos_entregues_total={payload['totals']['pedidos_entregues_total']}")
        print(f"pedidos_inseridos_total={payload['totals']['pedidos_inseridos_total']}")
        print(f"loggers_entregues_total={payload['totals']['loggers_entregues_total']}")
        print(f"loggers_inseridos_total={payload['totals']['loggers_inseridos_total']}")
        return 0
    except Exception as exc:
        print(f"erro={exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

