import os
import time
import logging
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Float, ForeignKey, DateTime, desc
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/bazaar_data")

# SQLAlchemy Setup
Base = declarative_base()

class RawBazaarData(Base):
    __tablename__ = 'raw_bazaar_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    lastUpdated = Column(BigInteger)
    data = Column(JSONB)

class Update(Base):
    __tablename__ = 'update'
    lastUpdated = Column(BigInteger, primary_key=True)
    timestamp = Column(BigInteger)

class Product(Base):
    __tablename__ = 'product'
    id = Column(String, primary_key=True)
    name = Column(String)

class ProductStatus(Base):
    __tablename__ = 'product_status'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    product_id = Column(String, ForeignKey('product.id'), index=True)
    update_id = Column(BigInteger, ForeignKey('update.lastUpdated'), index=True)
    
    sellPrice = Column(Float)
    sellVolume = Column(BigInteger)
    sellMovingWeek = Column(BigInteger)
    sellOrders = Column(BigInteger)
    
    buyPrice = Column(Float)
    buyVolume = Column(BigInteger)
    buyMovingWeek = Column(BigInteger)
    buyOrders = Column(BigInteger)

    product = relationship("Product")
    update = relationship("Update")
    sell_offers = relationship("SellOffer", back_populates="product_status")
    buy_offers = relationship("BuyOffer", back_populates="product_status")

class SellOffer(Base):
    __tablename__ = 'sellOffer'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    product_status_id = Column(BigInteger, ForeignKey('product_status.id'), index=True)
    amount = Column(BigInteger)
    pricePerUnit = Column(Float)
    orders = Column(BigInteger)
    
    product_status = relationship("ProductStatus", back_populates="sell_offers")

class BuyOffer(Base):
    __tablename__ = 'buyOffer'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    product_status_id = Column(BigInteger, ForeignKey('product_status.id'), index=True)
    amount = Column(BigInteger)
    pricePerUnit = Column(Float)
    orders = Column(BigInteger)

    product_status = relationship("ProductStatus", back_populates="buy_offers")

def get_postgres_session():
    engine = create_engine(POSTGRES_URI)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def main():
    logging.info("Starting SQL Processor Service (Postgres Source)...")
    
    # Wait for Postgres to be ready
    time.sleep(10) 
    
    session = get_postgres_session()
    
    while True:
        try:
            # Find the latest raw record from Postgres
            latest_raw_record = session.query(RawBazaarData).order_by(desc(RawBazaarData.id)).first()
            
            if latest_raw_record:
                raw_data = latest_raw_record.data
                raw_last_updated = raw_data.get("lastUpdated")
                
                # Check if this update already exists in Update table
                existing_update = session.query(Update).filter_by(lastUpdated=raw_last_updated).first()
                
                if not existing_update:
                    logging.info(f"Processing new data for SQL. lastUpdated: {raw_last_updated}")
                    
                    timestamp_val = int(latest_raw_record.timestamp.timestamp() * 1000)
                    
                    new_update = Update(lastUpdated=raw_last_updated, timestamp=timestamp_val)
                    session.add(new_update)
                    
                    products_data = raw_data.get("products", {})
                    
                    for product_name, product_info in products_data.items():
                        product_id = product_info.get("product_id")
                        
                        # Ensure Product exists
                        product = session.query(Product).filter_by(id=product_id).first()
                        if not product:
                            product = Product(id=product_id, name=product_name)
                            session.add(product)
                            session.flush()
                        
                        quick_status = product_info.get("quick_status", {})
                        
                        # Create ProductStatus
                        product_status = ProductStatus(
                            product_id=product_id,
                            update_id=raw_last_updated,
                            sellPrice=quick_status.get("sellPrice"),
                            sellVolume=quick_status.get("sellVolume"),
                            sellMovingWeek=quick_status.get("sellMovingWeek"),
                            sellOrders=quick_status.get("sellOrders"),
                            buyPrice=quick_status.get("buyPrice"),
                            buyVolume=quick_status.get("buyVolume"),
                            buyMovingWeek=quick_status.get("buyMovingWeek"),
                            buyOrders=quick_status.get("buyOrders")
                        )
                        session.add(product_status)
                        session.flush()
                        
                        # Process Sell Offers
                        for offer in product_info.get("sell_summary", []):
                            sell_offer = SellOffer(
                                product_status_id=product_status.id,
                                amount=offer.get("amount"),
                                pricePerUnit=offer.get("pricePerUnit"),
                                orders=offer.get("orders")
                            )
                            session.add(sell_offer)
                            
                        # Process Buy Offers
                        for offer in product_info.get("buy_summary", []):
                            buy_offer = BuyOffer(
                                product_status_id=product_status.id,
                                amount=offer.get("amount"),
                                pricePerUnit=offer.get("pricePerUnit"),
                                orders=offer.get("orders")
                            )
                            session.add(buy_offer)
                    
                    session.commit()
                    logging.info("SQL Processing complete.")
                else:
                    # logging.info("No new data for SQL.")
                    pass
            
        except Exception as e:
            logging.error(f"Error in SQL processor: {e}")
            session.rollback()
        
        time.sleep(10)

if __name__ == "__main__":
    main()
