import time
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, Column, Integer, DateTime, BigInteger
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

POSTGRES_URI = "postgresql://user:password@postgres:5432/bazaar_data"

Base = declarative_base()

class RawBazaarData(Base):
    __tablename__ = 'raw_bazaar_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    lastUpdated = Column(BigInteger)
    data = Column(JSONB)

def main():
    logging.info("Starting Verification Script (Postgres Only)...")
    
    engine = create_engine(POSTGRES_URI)
    Base.metadata.create_all(engine) # Ensure table exists
    Session = sessionmaker(bind=engine)
    session = Session()
    
    test_last_updated = int(time.time() * 1000)
    test_product_id = "TEST_PRODUCT_PG"
    
    dummy_data = {
        "success": True,
        "lastUpdated": test_last_updated,
        "products": {
            test_product_id: {
                "product_id": test_product_id,
                "quick_status": {
                    "sellPrice": 20.5,
                    "sellVolume": 200,
                    "sellMovingWeek": 1000,
                    "sellOrders": 10,
                    "buyPrice": 15.0,
                    "buyVolume": 400,
                    "buyMovingWeek": 1200,
                    "buyOrders": 20
                },
                "sell_summary": [
                    {"amount": 20, "pricePerUnit": 20.5, "orders": 2}
                ],
                "buy_summary": [
                    {"amount": 40, "pricePerUnit": 15.0, "orders": 4}
                ]
            }
        }
    }
    
    # 1. Insert dummy data into RawBazaarData table
    raw_record = RawBazaarData(
        timestamp=datetime.utcnow(),
        lastUpdated=test_last_updated,
        data=dummy_data
    )
    session.add(raw_record)
    session.commit()
    logging.info(f"Inserted dummy data into Postgres RawBazaarData with lastUpdated: {test_last_updated}")
    
    # 2. Wait for processor to pick it up
    logging.info("Waiting 40 seconds for processor...")
    time.sleep(40)
    
    # 3. Verify in Postgres Processed Tables
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM update WHERE \"lastUpdated\" = {test_last_updated}"))
        row = result.fetchone()
        
        if row:
            logging.info("✅ Verification SUCCESS: Update record found in Postgres.")
        else:
            logging.error("❌ Verification FAILED: Update record NOT found in Postgres.")
            
        # Check Product
        result = conn.execute(text(f"SELECT * FROM product WHERE id = '{test_product_id}'"))
        if result.fetchone():
             logging.info("✅ Verification SUCCESS: Product record found.")
        else:
             logging.error("❌ Verification FAILED: Product record NOT found.")

if __name__ == "__main__":
    main()
