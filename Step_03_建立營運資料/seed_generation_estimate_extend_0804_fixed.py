"""補齊 2026-03-20~2026-08-04 的模擬 fact_generation_estimate 資料。

此修正版專門處理 fact_generation_estimate 的 NOT NULL run_id/location_sk。
Windows PowerShell 執行時請設定 PVESPS_DB_HOST=localhost、PVESPS_DB_PORT=5433。
"""

from __future__ import annotations

import hashlib
import math
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2 import sql


START_DEFAULT = date(2026, 3, 20)
END_DEFAULT = date(2026, 8, 4)


def load_dotenv() -> None:
    for path in (
        Path(__file__).resolve().parents[1] / "Step_11_airflow_排程管理" / ".env",
        Path(__file__).resolve().parents[1] / ".env",
    ):
        if not path.exists():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return


def env(*names: str, default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def parse_date(name: str, default: date) -> date:
    value = os.getenv(name)
    return datetime.strptime(value, "%Y-%m-%d").date() if value else default


def connect():
    load_dotenv()
    return psycopg2.connect(
        host=env("PVESPS_DB_HOST", "DB_HOST", default="localhost"),
        port=int(env("PVESPS_DB_PORT", "DB_PORT", default="5433")),
        dbname=env("PVESPS_DB_NAME", "DB_NAME", "POSTGRES_DB", default="pvesps_dw"),
        user=env("PVESPS_DB_USER", "DB_USER", "POSTGRES_USER", default="postgres"),
        password=env("PVESPS_DB_PASSWORD", "DB_PASSWORD", "POSTGRES_PASSWORD", default="postgres"),
    )


def estimate(capacity: Decimal, efficiency_pct: Decimal, site_sk: int, target_date: date) -> Decimal:
    digest = hashlib.sha256(f"{site_sk}:{target_date.isoformat()}".encode()).digest()
    noise = (digest[0] / 255.0 - 0.5) * 0.12
    seasonal = 1.0 + 0.18 * math.sin(2 * math.pi * (target_date.timetuple().tm_yday - 80) / 365.25)
    efficiency = efficiency_pct / Decimal("100") if efficiency_pct > 1 else efficiency_pct
    value = capacity * Decimal("4.2") * efficiency * Decimal(str(max(0.35, seasonal + noise)))
    return value.quantize(Decimal("0.01"))


def main() -> int:
    start = parse_date("SEED_EXTEND_START", START_DEFAULT)
    end = parse_date("SEED_EXTEND_END", END_DEFAULT)
    if start > end:
        raise ValueError("SEED_EXTEND_START 不可晚於 SEED_EXTEND_END")

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema='mart' AND table_name='fact_generation_estimate'"""
            )
            columns = {row[0] for row in cur.fetchall()}
            required = {"site_sk", "location_sk", "run_id", "estimate_date", "estimated_generation_kwh"}
            missing = required - columns
            if missing:
                raise RuntimeError(f"fact_generation_estimate 缺少欄位：{', '.join(sorted(missing))}")

            cur.execute("SELECT MAX(run_id) FROM mart.fact_generation_estimate")
            run_id = cur.fetchone()[0]
            if run_id is None:
                raise RuntimeError("找不到可沿用的 run_id，請先建立有效 ETL run")

            cur.execute(
                """SELECT site_sk, location_sk, capacity_kw, install_area_ping,
                          COALESCE(baseline_efficiency_pct, 85)
                   FROM mart.dim_solar_site
                   WHERE site_sk IS NOT NULL ORDER BY site_sk"""
            )
            sites = cur.fetchall()
            cur.execute(
                """SELECT site_sk, estimate_date FROM mart.fact_generation_estimate
                   WHERE estimate_date BETWEEN %s AND %s""",
                (start, end),
            )
            existing = {(int(site_sk), target_date) for site_sk, target_date in cur.fetchall()}

            columns_to_insert = ["run_id", "site_sk", "location_sk", "estimate_date", "estimated_generation_kwh"]
            optional = [name for name in (
                "sunshine_hours", "install_area_ping", "formula_version",
                "data_source", "is_simulated", "loaded_at"
            ) if name in columns]
            columns_to_insert.extend(optional)

            rows = []
            current = start
            while current <= end:
                for site_sk_raw, location_sk, capacity_raw, area, efficiency_raw in sites:
                    site_sk = int(site_sk_raw)
                    if (site_sk, current) in existing:
                        continue
                    capacity = Decimal(str(capacity_raw or 0))
                    efficiency = Decimal(str(efficiency_raw or 85))
                    row = [run_id, site_sk, location_sk, current, estimate(capacity, efficiency, site_sk, current)]
                    for name in optional:
                        if name == "sunshine_hours":
                            row.append(Decimal("4.20"))
                        elif name == "install_area_ping":
                            row.append(area)
                        elif name == "formula_version":
                            row.append("seed_generation_estimate_extend_0804_v1")
                        elif name == "data_source":
                            row.append("SIMULATED")
                        elif name == "is_simulated":
                            row.append(True)
                        elif name == "loaded_at":
                            row.append(datetime.now())
                    rows.append(row)
                current += timedelta(days=1)

            if rows:
                statement = sql.SQL("INSERT INTO mart.fact_generation_estimate ({cols}) VALUES ({vals})").format(
                    cols=sql.SQL(", ").join(map(sql.Identifier, columns_to_insert)),
                    vals=sql.SQL(", ").join(sql.Placeholder() for _ in columns_to_insert),
                )
                cur.executemany(statement, rows)
            conn.commit()
            print(f"[SEED_ESTIMATE_EXTEND] range={start}..{end}; sites={len(sites)}; inserted_rows={len(rows)}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
