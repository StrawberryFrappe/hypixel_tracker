"""Application database metadata owned by the App migration track."""

from datetime import datetime

from sqlalchemy import DateTime, MetaData, SmallInteger, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "pk": "pk_%(table_name)s",
}


class AppBase(DeclarativeBase):
    metadata = MetaData(schema="app", naming_convention=NAMING_CONVENTION)


class AppScaffoldState(AppBase):
    """Migration-owned marker; serving tables arrive in a later phase."""

    __tablename__ = "scaffold_state"

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    initialized_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
