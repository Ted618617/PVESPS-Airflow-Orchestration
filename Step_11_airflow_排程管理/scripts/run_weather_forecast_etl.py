import os
from pathlib import Path
import runpy
if "DATABASE_URL" in os.environ:
    os.environ["DATABASE_URL"] = os.environ["DATABASE_URL"].replace("@localhost:", "@host.docker.internal:").replace("@127.0.0.1:", "@host.docker.internal:")
TARGET = Path("/opt/pvesps/Step_04_取得最近三日預報API/weather_forecast_etl.py")
if not TARGET.is_file():
    raise FileNotFoundError(f"Step 04 script not mounted: {TARGET}")
runpy.run_path(str(TARGET), run_name="__main__")


