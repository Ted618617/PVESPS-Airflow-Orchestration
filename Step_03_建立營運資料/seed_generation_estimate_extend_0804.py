"""
延伸模擬發電估算資料至 2026-08-04。

用途
----
seed_generation_estimate.py 已建立的資料不需重建，本腳本只補齊缺口
2026-03-20 ~ 2026-08-04，並以 site_sk + 日期檢查既有資料，避免重複寫入。

注意
----
本腳本產生的是展示/測試用模擬資料，不是真實量測值。若目標資料表有
data_source 或 is_simulated 欄位，會自動寫入 SIMULATED / true。

可用環境變數覆寫日期：
    SEED_EXTEND_START=2026-03-20
    SEED_EXTEND_END=2026-08-04

資料庫連線優先讀取：
    PVESPS_DB_HOST / PVESPS_DB_PORT / PVESPS_DB_NAME /
    PVESPS_DB_USER / PVESPS_DB_PASSWORD
其次支援一般 DB_HOST、DB_PORT、DB_NAME、DB_USER、DB_PASSWORD。
"""

from __future__ import annotations

import hashlib
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2 import sql


DEFAULT_START = date(2026, 3, 20)
DEFAULT_END = date(2026, 8, 4)


def _load_dotenv() -> None:
    """在本機執行時載入專案 .env；不覆寫已存在的環境變數。"""
    env_candidates = [
        Path(__file__).resolve().parents[1] / "Step_11_airflow_排程管理" / ".env",
        Path(__file__).resolve().parents[1] / ".env",
    ]
    for env_path in env_candidates:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
        break


def _env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def _parse_date(name: str, default: date) -> date:
    value = os.getenv(name)
    return datetime.strptime(value, "%Y-%m-%d").date() if value else default


def _connect():
    _load_dotenv()
    return psycopg2.connect(
        host=_env("PVESPS_DB_HOST", "DB_HOST", default="host.docker.internal"),
        port=int(_env("PVESPS_DB_PORT", "DB_PORT", default="5432")),
        dbname=_env("PVESPS_DB_NAME", "DB_NAME", "POSTGRES_DB", default="pvesps_dw"),
        user=_env("PVESPS_DB_USER", "DB_USER", "POSTGRES_USER", default="postgres"),
        password=_env("PVESPS_DB_PASSWORD", "DB_PASSWORD", "POSTGRES_PASSWORD", default="postgres"),
    )


def _find_column(columns: set[str], candidates: Iterable[str], label: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise RuntimeError(
        f"mart.fact_generation_estimate 找不到 {label} 欄位；"
        f"目前欄位：{', '.join(sorted(columns))}"
    )


def _stable_factor(site_sk: int, target_date: date) -> Decimal:
    """產生可重複執行的每日季節性係數，不使用不可重現的 random。"""
    digest = hashlib.sha256(f"{site_sk}:{target_date.isoformat()}".encode()).digest()
    noise = (digest[0] / 255.0 - 0.5) * 0.12
    day_of_year = target_date.timetuple().tm_yday
    seasonal = 1.0 + 0.18 * __import__("math").sin(
        2 * __import__("math").pi * (day_of_year - 80) / 365.25
    )
    return Decimal(str(max(0.35, seasonal + noise)))


def _estimate_kwh(capacity_kw: Decimal, efficiency_pct: Decimal, site_sk: int, target_date: date) -> Decimal:
    # 4.2 小時等效日照；與原始 seed 腳本相同用途，僅用於展示資料延伸。
    efficiency = efficiency_pct / Decimal("100") if efficiency_pct > 1 else efficiency_pct
    value = capacity_kw * Decimal("4.2") * efficiency * _stable_factor(site_sk, target_date)
    return value.quantize(Decimal("0.01"))


def main() -> int:
    start_date = _parse_date("SEED_EXTEND_START", DEFAULT_START)
    end_date = _parse_date("SEED_EXTEND_END", DEFAULT_END)
    if start_date > end_date:
        raise ValueError("SEED_EXTEND_START 不可晚於 SEED_EXTEND_END")

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'mart'
                  AND table_name = 'fact_generation_estimate'
                """
            )
            columns = {row[0] for row in cur.fetchall()}
            if not columns:
                raise RuntimeError("找不到 mart.fact_generation_estimate")

            date_col = _find_column(
                columns,
                ("estimate_date", "generation_date", "target_date", "prediction_date", "event_date"),
                "估算日期",
            )
            value_col = _find_column(
                columns,
                ("estimated_generation_kwh", "generation_estimate_kwh", "estimate_kwh", "generation_kwh"),
                "估算發電量",
            )
            if "site_sk" not in columns:
                raise RuntimeError("mart.fact_generation_estimate 找不到 site_sk 欄位")

            cur.execute(
                """
                SELECT site_sk, capacity_kw,
                       COALESCE(baseline_efficiency_pct, 85) AS efficiency_pct
                FROM mart.dim_solar_site
                WHERE site_sk IS NOT NULL
                ORDER BY site_sk
                """
            )
            sites = cur.fetchall()
            if not sites:
                raise RuntimeError("mart.dim_solar_site 沒有可用案場")

            # 只讀取目標期間既有 key；不刪除、不覆蓋使用者已存在的資料。
            cur.execute(
                sql.SQL(
                    "SELECT site_sk, {date_col}::date FROM mart.fact_generation_estimate "
                    "WHERE {date_col}::date BETWEEN %s AND %s"
                ).format(date_col=sql.Identifier(date_col)),
                (start_date, end_date),
            )
            existing = {(int(site_sk), target_date) for site_sk, target_date in cur.fetchall()}

            insert_columns = ["site_sk", date_col, value_col]
            insert_values = []
            optional_values = []
            for optional in ("data_source", "source", "is_simulated", "created_at", "loaded_at"):
                if optional in columns:
                    insert_columns.append(optional)
                    optional_values.append(optional)

            current = start_date
            while current <= end_date:
                for site_sk_raw, capacity_raw, efficiency_raw in sites:
                    site_sk = int(site_sk_raw)
                    if (site_sk, current) in existing:
                        continue
                    capacity = Decimal(str(capacity_raw or 0))
                    efficiency = Decimal(str(efficiency_raw or 85))
                    row = [site_sk, current, _estimate_kwh(capacity, efficiency, site_sk, current)]
                    for optional in optional_values:
                        if optional in ("data_source", "source"):
                            row.append("SIMULATED")
                        elif optional == "is_simulated":
                            row.append(True)
                        else:
                            row.append(datetime.now())
                    insert_values.append(row)
                current += timedelta(days=1)

            if insert_values:
                placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in insert_columns)
                statement = sql.SQL(
                    "INSERT INTO mart.fact_generation_estimate ({columns}) VALUES ({values})"
                ).format(
                    columns=sql.SQL(", ").join(map(sql.Identifier, insert_columns)),
                    values=placeholders,
                )
                cur.executemany(statement, insert_values)
            conn.commit()
            print(
                f"[SEED_ESTIMATE_EXTEND] range={start_date}..{end_date}; "
                f"sites={len(sites)}; inserted_rows={len(insert_values)}; skipped_existing={len(existing)}"
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
