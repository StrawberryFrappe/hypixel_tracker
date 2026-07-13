"""Create minimal Raw scaffold schema.

Revision ID: raw_0001
Revises:
"""

import sqlalchemy as sa
from alembic import op

from bazaar_guru.config import migration_runtime_role

revision: str = "raw_0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    runtime_role = _quoted_runtime_role()
    op.execute(sa.schema.CreateSchema("raw"))
    op.create_table(
        "scaffold_state",
        sa.Column("id", sa.SmallInteger(), nullable=False),
        sa.Column(
            "initialized_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_scaffold_state"),
        schema="raw",
    )
    op.execute(sa.text(f"GRANT USAGE ON SCHEMA raw TO {runtime_role}"))
    op.execute(sa.text(f"GRANT SELECT ON TABLE raw.scaffold_state TO {runtime_role}"))
    op.execute(sa.text(f"GRANT SELECT ON TABLE alembic_version_raw TO {runtime_role}"))


def downgrade() -> None:
    runtime_role = _quoted_runtime_role()
    op.execute(sa.text(f"REVOKE SELECT ON TABLE alembic_version_raw FROM {runtime_role}"))
    op.execute(sa.text(f"REVOKE SELECT ON TABLE raw.scaffold_state FROM {runtime_role}"))
    op.drop_table("scaffold_state", schema="raw")
    op.execute(sa.text(f"REVOKE USAGE ON SCHEMA raw FROM {runtime_role}"))
    op.execute(sa.schema.DropSchema("raw"))


def _quoted_runtime_role() -> str:
    role = migration_runtime_role("raw")
    return op.get_bind().dialect.identifier_preparer.quote(role)
