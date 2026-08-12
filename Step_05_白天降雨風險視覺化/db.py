from pathlib import Path
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

def get_engine() -> Engine:
    """
    Create SQLAlchemy engine using DATABASE_URL env variable.
    Example:
    postgresql+psycopg2://postgres:password@localhost:5433/pvesps_dw
    """
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise RuntimeError(
            "DATABASE_URL environment variable not set.\n"
            "Example:\n"
            "postgresql+psycopg2://postgres:s@localhost:5433/pvesps_dw"
        )

    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        future=True,
    )

    return engine