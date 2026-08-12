import os
from sqlalchemy import create_engine, text

url = os.environ.get("DATABASE_URL") or os.environ.get("PVESPS_DATABASE_URL")
if not url:
    raise RuntimeError("DATABASE_URL/PVESPS_DATABASE_URL is not configured")
url = url.replace("@localhost:", "@host.docker.internal:").replace("@127.0.0.1:", "@host.docker.internal:")

checks = {
    "weather_forecast": "SELECT count(*) FROM mart.fact_weather_forecast",
    "training_dataset": "SELECT count(*) FROM mart.ml_training_generation_daily",
    "prediction": "SELECT count(*) FROM mart.fact_generation_prediction_daily",
}
engine = create_engine(url, pool_pre_ping=True)
with engine.connect() as conn:
    for name, sql in checks.items():
        count = int(conn.execute(text(sql)).scalar_one())
        if count < 1:
            raise RuntimeError(f"quality check failed for {name}: table is empty")
        print(f"[QUALITY] {name}: {count} rows")
print("[QUALITY] all checks passed")
