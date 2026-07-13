"""Bounded SQLAlchemy engines and readiness probes."""

import logging
from dataclasses import dataclass

from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from bazaar_guru.config import DatabasePoolSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseReadiness:
    raw: bool
    app: bool


@dataclass(frozen=True)
class DatabaseProbe:
    version_table: str
    expected_revision: str
    required_table: str


RAW_DATABASE_PROBE = DatabaseProbe(
    version_table="alembic_version_raw",
    expected_revision="raw_0001",
    required_table="raw.scaffold_state",
)
APP_DATABASE_PROBE = DatabaseProbe(
    version_table="alembic_version_app",
    expected_revision="app_0001",
    required_table="app.scaffold_state",
)


def create_database_engine(database_url: SecretStr, settings: DatabasePoolSettings) -> Engine:
    common = {
        "pool_size": settings.database_pool_size,
        "max_overflow": settings.database_max_overflow,
        "pool_timeout": settings.database_pool_timeout_seconds,
        "pool_recycle": settings.database_pool_recycle_seconds,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": settings.database_connect_timeout_seconds},
    }
    return create_engine(database_url.get_secret_value(), **common)


def check_database_connection(engine: Engine, name: str, probe: DatabaseProbe) -> bool:
    try:
        with engine.connect() as connection:
            revision = connection.execute(
                text(f"SELECT version_num FROM {probe.version_table}")
            ).scalar_one_or_none()
            if revision != probe.expected_revision:
                logger.warning("%s database schema revision is incompatible", name)
                return False
            connection.execute(text(f"SELECT 1 FROM {probe.required_table} LIMIT 1"))
        return True
    except SQLAlchemyError as error:
        # Error class is useful operationally; exception text may contain a DSN.
        logger.warning("%s database readiness probe failed (%s)", name, type(error).__name__)
        return False
