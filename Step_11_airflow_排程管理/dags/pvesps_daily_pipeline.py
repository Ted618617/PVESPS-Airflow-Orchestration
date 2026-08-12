from __future__ import annotations
from datetime import timedelta
import subprocess
import pendulum
from airflow.decorators import dag, task

DEFAULT_ARGS = {"owner": "pvesps", "retries": 1, "retry_delay": timedelta(minutes=2)}

@dag(dag_id="pvesps_daily_pipeline", description="PVESPS daily ETL pipeline", default_args=DEFAULT_ARGS, start_date=pendulum.datetime(2026, 4, 1, tz="Asia/Taipei"), schedule=None, catchup=False, max_active_runs=1, tags=["PVESPS", "Airflow"])

def pvesps_daily_pipeline():
    @task(task_id="start")
    
    def start():
        print("[PVESPS] Pipeline started")
    def command_task(task_id, script):
        @task(task_id=task_id)
        def run():
            result = subprocess.run(["python", f"/opt/airflow/scripts/{script}"], check=True, text=True)
            return result.returncode
        return run()
    extract =           command_task("extract_weather_forecast", "run_weather_forecast_etl.py")
    snapshot =          command_task("snapshot_weather_forecast", "run_snapshot_weather_forecast.py")
    future_features =   command_task("build_future_features", "run_build_future_features.py")
    build =             command_task("build_training_dataset", "run_build_training_dataset.py")
    score =             command_task("score_generation_prediction", "run_score_generation_prediction.py")
    quality =           command_task("data_quality_check", "run_data_quality_check.py")
    @task(task_id="end")
    def end():
        print("[PVESPS] Pipeline completed successfully")
    start() >> extract >> snapshot >> future_features >> build >> score >> quality >> end()

pvesps_daily_pipeline()


