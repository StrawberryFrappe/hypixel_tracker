import gzip
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
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
)
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.orm import declarative_base, sessionmaker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bazaar.collector")

POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/bazaar_data")
API_URL = os.environ.get("HYPIXEL_BAZAAR_URL", "https://api.hypixel.net/v2/skyblock/bazaar")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "15"))
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "10"))

Base = declarative_base()


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


def get_session_factory():
    engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
    # The api and collector start together; tolerate the concurrent-create race
    # on Postgres catalog indexes instead of relying on the restart policy.
    for attempt in range(8):
        try:
            Base.metadata.create_all(engine)
            break
        except (IntegrityError, OperationalError, ProgrammingError) as exc:
            if attempt == 7:
                raise
            logger.warning("create_all contention (attempt %s): %s; retrying", attempt + 1, exc)
            time.sleep(1)
    return sessionmaker(bind=engine)


def fetch_bazaar_response() -> tuple[int, bytes] | None:
    try:
        response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        logger.warning("Hypixel fetch failed: %s", exc)
        return None

    if response.status_code != 200:
        logger.warning("Hypixel returned status %s", response.status_code)
        return response.status_code, response.content
    return response.status_code, response.content


def parse_last_updated(raw_bytes: bytes) -> int | None:
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("Hypixel response was not valid JSON: %s", exc)
        return None
    if not data.get("success"):
        logger.warning("Hypixel response success flag was false")
        return None
    last_updated = data.get("lastUpdated")
    if not isinstance(last_updated, int):
        logger.warning("Hypixel response did not include integer lastUpdated")
        return None
    return last_updated


def store_snapshot(session, status_code: int, raw_bytes: bytes) -> bool:
    last_updated = parse_last_updated(raw_bytes)
    if last_updated is None:
        return False

    response_hash = hashlib.sha256(raw_bytes).hexdigest()
    latest = session.query(RawSnapshot).order_by(desc(RawSnapshot.id)).first()
    if latest and (latest.source_last_updated == last_updated or latest.response_hash == response_hash):
        logger.info("Duplicate Bazaar snapshot skipped: lastUpdated=%s", last_updated)
        return False

    compressed = gzip.compress(raw_bytes, compresslevel=9)
    snapshot = RawSnapshot(
        endpoint="/v2/skyblock/bazaar",
        fetched_at=datetime.now(timezone.utc),
        source_last_updated=last_updated,
        response_hash=response_hash,
        status_code=status_code,
        payload_size_bytes=len(raw_bytes),
        compressed_size_bytes=len(compressed),
        payload_gzip=compressed,
    )
    session.add(snapshot)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        logger.info("Duplicate Bazaar snapshot rejected by database: lastUpdated=%s", last_updated)
        return False
    logger.info(
        "Stored Bazaar snapshot id=%s lastUpdated=%s raw=%s compressed=%s",
        snapshot.id,
        last_updated,
        len(raw_bytes),
        len(compressed),
    )
    return True


def main() -> None:
    logger.info("Starting Hypixel Bazaar raw collector")
    session_factory = get_session_factory()

    while True:
        started = time.time()
        session = session_factory()
        try:
            result = fetch_bazaar_response()
            if result is not None:
                status_code, raw_bytes = result
                if status_code == 200:
                    store_snapshot(session, status_code, raw_bytes)
        except Exception:
            logger.exception("Collector loop failed")
            session.rollback()
        finally:
            session.close()

        elapsed = time.time() - started
        time.sleep(max(POLL_INTERVAL_SECONDS - elapsed, 1))


if __name__ == "__main__":
    main()
