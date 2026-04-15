from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import declarative_base, sessionmaker

from powerbuddy.config import settings


Base = declarative_base()
engine = create_engine(f"sqlite:///{settings.db_path}", future=True)
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)


def init_db() -> None:
    from powerbuddy import models  # noqa: F401

    Base.metadata.create_all(bind=engine)

    # Lightweight SQLite migration for new plan-actions fields.
    with engine.begin() as conn:
        cols = conn.execute(text("PRAGMA table_info(plan_actions)")).fetchall()
        names = {row[1] for row in cols}
        if "charge_power_w" not in names:
            conn.execute(text("ALTER TABLE plan_actions ADD COLUMN charge_power_w FLOAT"))

        # Data migration: planner no longer emits "discharge". Keep manual discharge rows,
        # but convert non-manual legacy entries to "auto".
        conn.execute(
            text(
                """
                UPDATE plan_actions
                SET action = 'auto'
                WHERE action = 'discharge'
                  AND COALESCE(is_manual_override, 0) = 0
                """
            )
        )
