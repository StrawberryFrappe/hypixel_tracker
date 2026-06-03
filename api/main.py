import gzip
import hashlib
import json
import logging
import os
import secrets
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
    create_engine,
    desc,
    func,
    text,
)
from sqlalchemy.orm import declarative_base, sessionmaker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bazaar.api")

POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/bazaar_data")
API_KEY = os.environ.get("API_KEY", "dev-api-key")
EXPORT_DIR = Path(os.environ.get("EXPORT_DIR", "/exports"))
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
BACKUP_CACHE_BYTES = int(os.environ.get("BACKUP_CACHE_BYTES", str(25 * 1024**3)))
DISK_WARN_PERCENT = float(os.environ.get("DISK_WARN_PERCENT", "80"))
SNAPSHOT_INTERVAL_SECONDS = int(os.environ.get("SNAPSHOT_INTERVAL_SECONDS", "20"))
AGENT_DB_USER = os.environ.get("AGENT_DB_USER", "agent")
AGENT_DB_PASSWORD = os.environ.get("AGENT_DB_PASSWORD", "")

Base = declarative_base()
engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


class RawSnapshot(Base):
    __tablename__ = "raw_bazaar_snapshots"
    __table_args__ = (
        UniqueConstraint("source_last_updated", name="uq_bazaar_source_last_updated"),
        UniqueConstraint("response_hash", name="uq_bazaar_response_hash"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    endpoint = Column(String, nullable=False, default="/v2/skyblock/bazaar")
    fetched_at = Column(DateTime(timezone=True), nullable=False)
    source_last_updated = Column(BigInteger, nullable=False, index=True)
    response_hash = Column(String(64), nullable=False, index=True)
    status_code = Column(Integer, nullable=False)
    payload_size_bytes = Column(Integer, nullable=False)
    compressed_size_bytes = Column(Integer, nullable=False)
    payload_gzip = Column(LargeBinary, nullable=False)


class ExportBundle(Base):
    __tablename__ = "export_bundles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    start_snapshot_id = Column(Integer, nullable=False)
    end_snapshot_id = Column(Integer, nullable=False)
    start_source_last_updated = Column(BigInteger, nullable=False)
    end_source_last_updated = Column(BigInteger, nullable=False)
    snapshot_count = Column(Integer, nullable=False)
    file_name = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    file_size_bytes = Column(BigInteger, nullable=False)
    sha256 = Column(String(64), nullable=False)
    download_token = Column(String(96), nullable=False, unique=True)
    expires_at = Column(DateTime(timezone=True), nullable=False)


def init_db() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(engine)


def provision_sandbox() -> None:
    """Create a least-privilege `agent` role + `sandbox` schema it owns.

    The agent may read the immutable canonical archive but can only write inside
    `sandbox`. Idempotent; runs as the admin superuser the API connects with.
    """
    if not AGENT_DB_PASSWORD:
        logger.warning("AGENT_DB_PASSWORD not set; skipping sandbox provisioning")
        return
    from psycopg2 import sql as pgsql

    role = pgsql.Identifier(AGENT_DB_USER)
    pw = pgsql.Literal(AGENT_DB_PASSWORD)
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (AGENT_DB_USER,))
        if cur.fetchone() is None:
            cur.execute(pgsql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(role, pw))
        else:
            cur.execute(pgsql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(role, pw))
        cur.execute(pgsql.SQL("CREATE SCHEMA IF NOT EXISTS sandbox AUTHORIZATION {}").format(role))
        cur.execute(pgsql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
        cur.execute(
            pgsql.SQL(
                "GRANT SELECT ON public.raw_bazaar_snapshots, public.export_bundles TO {}"
            ).format(role)
        )
        # Defense in depth: ensure the agent can never create objects in public.
        cur.execute(pgsql.SQL("REVOKE CREATE ON SCHEMA public FROM {}").format(role))
        cur.close()
        raw.commit()
        logger.info("Sandbox provisioned: role %s + schema sandbox", AGENT_DB_USER)
    finally:
        raw.close()


app = FastAPI(title="Hypixel Bazaar Raw Archive", version="0.1.0")


@app.on_event("startup")
def startup() -> None:
    init_db()
    provision_sandbox()
    if API_KEY == "dev-api-key":
        logger.warning("Using default API_KEY. Set a strong API_KEY before deployment.")


def require_api_key(x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")) -> None:
    if not x_api_key or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def session_scope():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def snapshot_meta(snapshot: RawSnapshot) -> dict:
    return {
        "id": snapshot.id,
        "endpoint": snapshot.endpoint,
        "fetched_at": snapshot.fetched_at.isoformat(),
        "source_last_updated": snapshot.source_last_updated,
        "response_hash": snapshot.response_hash,
        "status_code": snapshot.status_code,
        "payload_size_bytes": snapshot.payload_size_bytes,
        "compressed_size_bytes": snapshot.compressed_size_bytes,
    }


def export_meta(bundle: ExportBundle, request: Optional[Request] = None) -> dict:
    base = PUBLIC_BASE_URL
    if not base and request is not None:
        base = str(request.base_url).rstrip("/")
    download_url = f"{base}/exports/{bundle.id}?token={bundle.download_token}" if base else None
    return {
        "id": bundle.id,
        "created_at": bundle.created_at.isoformat(),
        "snapshot_count": bundle.snapshot_count,
        "start_snapshot_id": bundle.start_snapshot_id,
        "end_snapshot_id": bundle.end_snapshot_id,
        "start_source_last_updated": bundle.start_source_last_updated,
        "end_source_last_updated": bundle.end_source_last_updated,
        "file_name": bundle.file_name,
        "file_size_bytes": bundle.file_size_bytes,
        "sha256": bundle.sha256,
        "expires_at": bundle.expires_at.isoformat(),
        "download_url": download_url,
    }


def delete_expired_and_pressure_exports(session) -> None:
    now = datetime.now(timezone.utc)
    expired = session.query(ExportBundle).filter(ExportBundle.expires_at <= now).all()
    for bundle in expired:
        Path(bundle.file_path).unlink(missing_ok=True)
        session.delete(bundle)
    session.commit()

    bundles = session.query(ExportBundle).order_by(ExportBundle.created_at.asc()).all()
    total = sum(Path(bundle.file_path).stat().st_size for bundle in bundles if Path(bundle.file_path).exists())
    for bundle in bundles:
        if total <= BACKUP_CACHE_BYTES:
            break
        path = Path(bundle.file_path)
        size = path.stat().st_size if path.exists() else 0
        path.unlink(missing_ok=True)
        total -= size
        session.delete(bundle)
    session.commit()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/snapshots", dependencies=[Depends(require_api_key)])
def list_snapshots(
    limit: int = Query(100, ge=1, le=1000),
    before_id: Optional[int] = None,
    session=Depends(session_scope),
) -> dict:
    query = session.query(RawSnapshot).order_by(desc(RawSnapshot.id))
    if before_id is not None:
        query = query.filter(RawSnapshot.id < before_id)
    snapshots = query.limit(limit).all()
    return {"snapshots": [snapshot_meta(snapshot) for snapshot in snapshots]}


@app.get("/snapshots/latest", dependencies=[Depends(require_api_key)])
def latest_snapshot(session=Depends(session_scope)) -> dict:
    snapshot = session.query(RawSnapshot).order_by(desc(RawSnapshot.id)).first()
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshots found")
    payload = json.loads(gzip.decompress(snapshot.payload_gzip).decode("utf-8"))
    return {"metadata": snapshot_meta(snapshot), "payload": payload}


@app.get("/snapshots/{snapshot_id}", dependencies=[Depends(require_api_key)])
def get_snapshot(snapshot_id: int, session=Depends(session_scope)) -> dict:
    snapshot = session.get(RawSnapshot, snapshot_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    payload = json.loads(gzip.decompress(snapshot.payload_gzip).decode("utf-8"))
    return {"metadata": snapshot_meta(snapshot), "payload": payload}


@app.get("/storage/status", dependencies=[Depends(require_api_key)])
def storage_status(session=Depends(session_scope)) -> dict:
    count = session.query(func.count(RawSnapshot.id)).scalar() or 0
    avg_compressed = session.query(func.avg(RawSnapshot.compressed_size_bytes)).scalar() or 442048
    oldest = session.query(RawSnapshot).order_by(RawSnapshot.id.asc()).first()
    newest = session.query(RawSnapshot).order_by(desc(RawSnapshot.id)).first()

    db_size = session.execute(text("SELECT pg_database_size(current_database())")).scalar()
    disk = shutil.disk_usage(EXPORT_DIR)
    used_percent = round((disk.used / disk.total) * 100, 2)
    snapshots_per_day = max(1, int(86400 / SNAPSHOT_INTERVAL_SECONDS))
    estimated_daily_bytes = int(avg_compressed * snapshots_per_day)
    estimated_days_remaining = int(disk.free / estimated_daily_bytes) if estimated_daily_bytes else None

    return {
        "snapshot_count": count,
        "oldest_snapshot": snapshot_meta(oldest) if oldest else None,
        "newest_snapshot": snapshot_meta(newest) if newest else None,
        "database_size_bytes": db_size,
        "avg_compressed_snapshot_bytes": int(avg_compressed),
        "estimated_daily_archive_bytes": estimated_daily_bytes,
        "disk": {
            "total_bytes": disk.total,
            "used_bytes": disk.used,
            "free_bytes": disk.free,
            "used_percent": used_percent,
            "warning": used_percent >= DISK_WARN_PERCENT,
        },
        "estimated_days_remaining_at_current_average": estimated_days_remaining,
    }


@app.get("/exports", dependencies=[Depends(require_api_key)])
def list_exports(request: Request, session=Depends(session_scope)) -> dict:
    delete_expired_and_pressure_exports(session)
    bundles = session.query(ExportBundle).order_by(desc(ExportBundle.created_at)).all()
    return {"exports": [export_meta(bundle, request) for bundle in bundles]}


@app.post("/exports", dependencies=[Depends(require_api_key)])
def create_export(
    request: Request,
    start_id: Optional[int] = Query(default=None, ge=1),
    end_id: Optional[int] = Query(default=None, ge=1),
    days: Optional[int] = Query(default=7, ge=1, le=90),
    expires_in_days: int = Query(default=7, ge=1, le=30),
    session=Depends(session_scope),
) -> dict:
    delete_expired_and_pressure_exports(session)

    query = session.query(RawSnapshot)
    if start_id is not None:
        query = query.filter(RawSnapshot.id >= start_id)
    if end_id is not None:
        query = query.filter(RawSnapshot.id <= end_id)
    if start_id is None and end_id is None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days or 7)
        query = query.filter(RawSnapshot.fetched_at >= cutoff)
    ordered = query.order_by(RawSnapshot.id.asc())
    first = ordered.first()
    if not first:
        raise HTTPException(status_code=404, detail="No snapshots found for export range")
    last = query.order_by(desc(RawSnapshot.id)).first()
    snapshot_count = query.count()
    created_at = datetime.now(timezone.utc)
    file_name = (
        f"bazaar_{first.source_last_updated}_{last.source_last_updated}_"
        f"{created_at.strftime('%Y%m%d%H%M%S')}.jsonl.gz"
    )
    file_path = EXPORT_DIR / file_name

    sha = hashlib.sha256()
    with gzip.open(file_path, "wt", encoding="utf-8") as fh:
        for snapshot in ordered.yield_per(100):
            payload_text = gzip.decompress(snapshot.payload_gzip).decode("utf-8")
            record = {"metadata": snapshot_meta(snapshot), "payload": json.loads(payload_text)}
            line = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
            fh.write(line + "\n")

    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)

    bundle = ExportBundle(
        created_at=created_at,
        start_snapshot_id=first.id,
        end_snapshot_id=last.id,
        start_source_last_updated=first.source_last_updated,
        end_source_last_updated=last.source_last_updated,
        snapshot_count=snapshot_count,
        file_name=file_name,
        file_path=str(file_path),
        file_size_bytes=file_path.stat().st_size,
        sha256=sha.hexdigest(),
        download_token=secrets.token_urlsafe(48),
        expires_at=created_at + timedelta(days=expires_in_days),
    )
    session.add(bundle)
    session.commit()
    session.refresh(bundle)
    return {"export": export_meta(bundle, request)}


@app.get("/exports/{export_id}")
def download_export(export_id: int, token: str, session=Depends(session_scope)):
    bundle = session.get(ExportBundle, export_id)
    if not bundle:
        raise HTTPException(status_code=404, detail="Export not found")
    if bundle.expires_at <= datetime.now(timezone.utc):
        raise HTTPException(status_code=410, detail="Export expired")
    if not secrets.compare_digest(token, bundle.download_token):
        raise HTTPException(status_code=401, detail="Invalid download token")
    path = Path(bundle.file_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Export file missing")
    return FileResponse(path, media_type="application/gzip", filename=bundle.file_name)
