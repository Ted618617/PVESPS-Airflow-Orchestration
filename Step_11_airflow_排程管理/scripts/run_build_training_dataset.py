import os
from pathlib import Path
import runpy
if "DATABASE_URL" in os.environ:
    os.environ["DATABASE_URL"] = os.environ["DATABASE_URL"].replace("@localhost:", "@host.docker.internal:").replace("@127.0.0.1:", "@host.docker.internal:")
TARGET = Path("/opt/pvesps/Step_06_建立ML訓練資料集/build_training_dataset.py")
if not TARGET.is_file():
    raise FileNotFoundError(f"Step 06 script not mounted: {TARGET}")
runpy.run_path(str(TARGET), run_name="__main__")


