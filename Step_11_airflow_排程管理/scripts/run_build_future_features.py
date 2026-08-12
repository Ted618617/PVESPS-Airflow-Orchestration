"""建立 CWA 預報對應的未來 ML 特徵；不使用 Cut-off 之後的實際發電資料。"""
import os
from sqlalchemy import create_engine, text

url = (os.environ.get("DATABASE_URL") or os.environ.get("PVESPS_DATABASE_URL"))
url = url.replace("@localhost:", "@host.docker.internal:").replace("@127.0.0.1:", "@host.docker.internal:")
engine = create_engine(url, pool_pre_ping=True)
sql = text("""
WITH cutoff AS (
    SELECT COALESCE(MAX(training_date), DATE '1900-01-01') AS d
    FROM mart.ml_training_generation_daily
    WHERE target_available = TRUE
) , daily_weather AS (
    SELECT location_sk, forecast_start_time::date AS d,
           AVG(pop_value) AS pop_value, MAX(pop_type) AS pop_type,
           MAX(forecast_issue_time) AS issue_time
    FROM mart.fact_weather_forecast
    GROUP BY location_sk, forecast_start_time::date
)
INSERT INTO mart.ml_training_generation_daily (
    run_id, site_sk, location_sk, training_date, target_generation_kwh,
    target_available, target_type, year_num, month_num, day_num, day_of_year,
    week_of_year, weekday_num, is_weekend, season_code, install_area_ping,
    capacity_kw, panel_efficiency, site_region, site_county, site_type,
    sunshine_hours, sunshine_rate_pct, solar_radiation_mj_m2,
    pop_value, pop_type, forecast_issue_time, rain_risk_flag, cloudy_risk_flag,
    estimated_generation_rule_kwh, generation_per_ping, sunshine_x_area,
    radiation_x_area, pop_x_sunshine, lag_1_generation_kwh,
    lag_3_avg_generation_kwh, lag_7_avg_generation_kwh, lag_14_avg_generation_kwh,
    lag_1_sunshine_hours, lag_3_avg_sunshine_hours, feature_missing_cnt,
    is_valid_for_training, invalid_reason
 )
SELECT
    :run_id, s.site_sk, s.location_sk, w.d, NULL, FALSE, 'simulated',
    EXTRACT(YEAR FROM w.d)::int, EXTRACT(MONTH FROM w.d)::int, EXTRACT(DAY FROM w.d)::int,
    EXTRACT(DOY FROM w.d)::int, EXTRACT(WEEK FROM w.d)::int, EXTRACT(ISODOW FROM w.d)::int,
    (EXTRACT(ISODOW FROM w.d) IN (6,7)),
    CASE WHEN EXTRACT(MONTH FROM w.d) IN (3,4,5) THEN 'SPRING' WHEN EXTRACT(MONTH FROM w.d) IN (6,7,8) THEN 'SUMMER' WHEN EXTRACT(MONTH FROM w.d) IN (9,10,11) THEN 'AUTUMN' ELSE 'WINTER' END,
    s.install_area_ping, s.capacity_kw, s.baseline_efficiency_pct / 100.0,
    s.city_name, s.county_name, 'solar_site', NULL, NULL, NULL, w.pop_value, w.pop_type, w.issue_time,
    FALSE, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, TRUE, 'CWA day_ahead feature; target intentionally unavailable'
FROM daily_weather w JOIN mart.dim_solar_site s ON s.location_sk=w.location_sk CROSS JOIN cutoff c
WHERE w.d > c.d
ON CONFLICT (site_sk, training_date) DO UPDATE SET
    pop_value=EXCLUDED.pop_value, pop_type=EXCLUDED.pop_type, forecast_issue_time=EXCLUDED.forecast_issue_time,
    target_available=FALSE, invalid_reason=EXCLUDED.invalid_reason, loaded_at=CURRENT_TIMESTAMP
RETURNING site_sk
""")
with engine.begin() as conn:
    rows = conn.execute(sql, {"run_id": int(os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "0").replace("manual__", "0")[:0] or 0)}).rowcount
print(f"[FUTURE_FEATURES] upserted rows={rows}")



