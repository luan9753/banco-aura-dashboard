import os
import sys
from datetime import datetime
from types import SimpleNamespace

from gerar_dashboard_entregas import (
    DEFAULT_DATABASE,
    DEFAULT_HOST,
    DEFAULT_PASSWORD,
    DEFAULT_PORT,
    DEFAULT_START_DATE,
    DEFAULT_USER,
    build_payload,
    get_connection,
    query_data,
    render_html,
)


def main() -> int:
    # Script dedicado: gera sempre o mesmo HTML de acompanhamento.
    args = SimpleNamespace(
        host=os.getenv("AURA_DB_HOST", DEFAULT_HOST),
        database=os.getenv("AURA_DB_NAME", DEFAULT_DATABASE),
        user=os.getenv("AURA_DB_USER", DEFAULT_USER),
        password=os.getenv("AURA_DB_PASSWORD", DEFAULT_PASSWORD),
        port=int(os.getenv("AURA_DB_PORT", DEFAULT_PORT)),
    )
    start_date = os.getenv("AURA_START_DATE", DEFAULT_START_DATE)
    end_date_env = os.getenv("AURA_END_DATE", "").strip()
    end_date = end_date_env or datetime.now().strftime("%Y-%m-%d")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(base_dir, "HTMLACOMPANHAMENTO.html")

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

        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"arquivo={output_path}")
        print(f"periodo_desde={start_date}")
        print(f"periodo_ate={end_date}")
        return 0
    except Exception as exc:
        print(f"erro={exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
