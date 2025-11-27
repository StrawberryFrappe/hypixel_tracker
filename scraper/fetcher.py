import os
import time
import requests
import logging
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import desc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/bazaar_data")
API_URL = "https://api.hypixel.net/v2/skyblock/bazaar"

Base = declarative_base()

class RawBazaarData(Base):
    __tablename__ = 'raw_bazaar_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    lastUpdated = Column(BigInteger)
    data = Column(JSONB)

def get_db_session():
    engine = create_engine(POSTGRES_URI)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def fetch_bazaar_data():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None

def main():
    logging.info("Starting Fetcher Service (Postgres)...")
    
    # Wait for Postgres
    time.sleep(10)
    
    session = get_db_session()
    
    while True:
        start_time = time.time()
        
        data = fetch_bazaar_data()
        if data and data.get("success"):
            last_updated = data.get("lastUpdated")
            
            # Check if we already have this lastUpdated
            # We query the latest record's data->lastUpdated
            # Note: Querying JSONB can be slower without index, but for now this is fine.
            # Alternatively, we can just check the latest record by ID and see its content.
            
            latest_record = session.query(RawBazaarData).order_by(desc(RawBazaarData.id)).first()
            
            is_new = True
            if latest_record:
                stored_last_updated = latest_record.data.get("lastUpdated")
                if stored_last_updated == last_updated:
                    is_new = False
            
            if is_new:
                raw_record = RawBazaarData(
                    timestamp=datetime.utcnow(),
                    lastUpdated=last_updated,
                    data=data
                )
                try:
                    session.add(raw_record)
                    session.commit()
                    logging.info(f"New data stored. lastUpdated: {last_updated}")
                except Exception as e:
                    logging.error(f"Error storing raw data: {e}")
                    session.rollback()
            else:
                logging.info(f"Duplicate data detected. lastUpdated: {last_updated}. Skipping.")
        else:
            logging.warning("Failed to fetch data or success is False")
        
        # Sleep logic
        elapsed = time.time() - start_time
        sleep_time = max(15 - elapsed, 1)
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
