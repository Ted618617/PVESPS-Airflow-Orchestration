import os
from pathlib import Path
import runpy
if "DATABASE_URL" in os.environ:
    os.environ["DATABASE_URL"] = os.environ["DATABASE_URL"].replace("@localhost:", "@host.docker.internal:").replace("@127.0.0.1:", "@host.docker.internal:")
TARGET = Path("/opt/pvesps/Step_08_批次推論與結果落庫/score_generation_prediction.py")
if not TARGET.is_file():
    raise FileNotFoundError(f"Step 08 script not mounted: {TARGET}")
runpy.run_path(str(TARGET), run_name="__main__")


