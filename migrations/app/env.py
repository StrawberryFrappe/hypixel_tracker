from alembic import context
from sqlalchemy import create_engine, pool

from bazaar_guru.config import migration_database_url
from bazaar_guru.db.app import AppBase

config = context.config
target_metadata = AppBase.metadata
version_table = config.get_main_option("version_table")


def run_migrations_offline() -> None:
    context.configure(
        url=migration_database_url("app"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        version_table=version_table,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(migration_database_url("app"), poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            version_table=version_table,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
