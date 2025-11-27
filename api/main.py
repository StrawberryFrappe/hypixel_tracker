import os
import logging
from typing import Optional, List, Union
from datetime import datetime
from dateutil import parser as date_parser
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Float, ForeignKey, desc, and_
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, joinedload

# Configure logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# Environment variables
POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/bazaar_data")

# SQLAlchemy Setup (Mirrored from sql_processor.py)
Base = declarative_base()

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

def get_db_session():
    engine = create_engine(POSTGRES_URI)
    Session = sessionmaker(bind=engine)
    return Session()

def parse_time_param(param: Union[int, str, None]) -> Optional[int]:
    if param is None:
        return None
    if isinstance(param, int):
        return param
    if isinstance(param, str):
        if param.isdigit():
            return int(param)
        try:
            dt = date_parser.parse(param)
            return int(dt.timestamp() * 1000)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {param}. Error: {e}")
    return None

def parse_lookback(lookback_str: str) -> int:
    try:
        parts = list(map(int, lookback_str.split(':')))
        if len(parts) != 5:
            raise ValueError("Invalid format")
        months, days, hours, minutes, seconds = parts
        
        # Approximate month as 30 days
        total_seconds = (
            months * 30 * 24 * 3600 +
            days * 24 * 3600 +
            hours * 3600 +
            minutes * 60 +
            seconds
        )
        return total_seconds * 1000 # Convert to ms
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid lookback format. Expected MM:DD:HH:MM:SS. Error: {e}")

@app.get("/latest")
def get_latest_bazaar_data():
    session = get_db_session()
    try:
        # Get the latest update
        latest_update = session.query(Update).order_by(desc(Update.lastUpdated)).first()
        
        if not latest_update:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Get all product statuses for this update
        statuses = session.query(ProductStatus).options(joinedload(ProductStatus.product)).filter_by(update_id=latest_update.lastUpdated).all()
        
        products_dict = {}
        for status in statuses:
            products_dict[status.product_id] = {
                "product_id": status.product_id,
                "name": status.product.name if status.product else status.product_id,
                "quick_status": {
                    "sellPrice": status.sellPrice,
                    "sellVolume": status.sellVolume,
                    "sellMovingWeek": status.sellMovingWeek,
                    "sellOrders": status.sellOrders,
                    "buyPrice": status.buyPrice,
                    "buyVolume": status.buyVolume,
                    "buyMovingWeek": status.buyMovingWeek,
                    "buyOrders": status.buyOrders
                }
            }
            
        return {
            "lastUpdated": latest_update.lastUpdated,
            "timestamp": latest_update.timestamp,
            "product_count": len(products_dict),
            "products": products_dict
        }
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/products")
def get_products():
    session = get_db_session()
    try:
        products = session.query(Product).all()
        return [{"id": p.id, "name": p.name} for p in products]
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/products/{product_id}/status")
def get_product_status_history(
    product_id: str,
    start: Union[int, str, None] = Query(None, description="Start timestamp (ms) or ISO date string"),
    end: Union[int, str, None] = Query(None, description="End timestamp (ms) or ISO date string"),
    lookback: Optional[str] = Query(None, description="Lookback duration in MM:DD:HH:MM:SS format"),
    limit: int = Query(100, le=1000, description="Max records to return")
):
    session = get_db_session()
    try:
        start_ts = parse_time_param(start)
        end_ts = parse_time_param(end)
        
        if lookback:
            lookback_ms = parse_lookback(lookback)
            if end_ts:
                start_ts = end_ts - lookback_ms
            else:
                # If end is not provided, default to now
                now_ms = int(datetime.utcnow().timestamp() * 1000)
                start_ts = now_ms - lookback_ms
                # We don't necessarily set end_ts to now, as we want all data up to now (or whatever the latest is)

        query = session.query(ProductStatus).join(Update).filter(ProductStatus.product_id == product_id)
        
        if start_ts:
            query = query.filter(Update.timestamp >= start_ts)
        if end_ts:
            query = query.filter(Update.timestamp <= end_ts)
            
        # Order by timestamp desc
        query = query.order_by(desc(Update.timestamp))
        
        statuses = query.limit(limit).all()
        
        result = []
        for status in statuses:
            result.append({
                "timestamp": status.update.timestamp,
                "lastUpdated": status.update.lastUpdated,
                "sellPrice": status.sellPrice,
                "sellVolume": status.sellVolume,
                "sellOrders": status.sellOrders,
                "buyPrice": status.buyPrice,
                "buyVolume": status.buyVolume,
                "buyOrders": status.buyOrders
            })
            
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/products/{product_id}/buy-offers")
def get_product_buy_offers(
    product_id: str,
    start: Union[int, str, None] = Query(None, description="Start timestamp (ms) or ISO date string"),
    end: Union[int, str, None] = Query(None, description="End timestamp (ms) or ISO date string"),
    lookback: Optional[str] = Query(None, description="Lookback duration in MM:DD:HH:MM:SS format"),
    limit: int = Query(100, le=1000, description="Max records to return")
):
    session = get_db_session()
    try:
        start_ts = parse_time_param(start)
        end_ts = parse_time_param(end)

        if lookback:
            lookback_ms = parse_lookback(lookback)
            if end_ts:
                start_ts = end_ts - lookback_ms
            else:
                now_ms = int(datetime.utcnow().timestamp() * 1000)
                start_ts = now_ms - lookback_ms

        # Join ProductStatus and Update to filter by time and product
        query = session.query(BuyOffer).join(ProductStatus).join(Update).filter(ProductStatus.product_id == product_id)
        
        if start_ts:
            query = query.filter(Update.timestamp >= start_ts)
        if end_ts:
            query = query.filter(Update.timestamp <= end_ts)
            
        query = query.order_by(desc(Update.timestamp))
        
        offers = query.limit(limit).all()
        
        result = []
        for offer in offers:
            result.append({
                "timestamp": offer.product_status.update.timestamp,
                "amount": offer.amount,
                "pricePerUnit": offer.pricePerUnit,
                "orders": offer.orders
            })
            
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/products/{product_id}/sell-offers")
def get_product_sell_offers(
    product_id: str,
    start: Union[int, str, None] = Query(None, description="Start timestamp (ms) or ISO date string"),
    end: Union[int, str, None] = Query(None, description="End timestamp (ms) or ISO date string"),
    lookback: Optional[str] = Query(None, description="Lookback duration in MM:DD:HH:MM:SS format"),
    limit: int = Query(100, le=1000, description="Max records to return")
):
    session = get_db_session()
    try:
        start_ts = parse_time_param(start)
        end_ts = parse_time_param(end)

        if lookback:
            lookback_ms = parse_lookback(lookback)
            if end_ts:
                start_ts = end_ts - lookback_ms
            else:
                now_ms = int(datetime.utcnow().timestamp() * 1000)
                start_ts = now_ms - lookback_ms

        query = session.query(SellOffer).join(ProductStatus).join(Update).filter(ProductStatus.product_id == product_id)
        
        if start_ts:
            query = query.filter(Update.timestamp >= start_ts)
        if end_ts:
            query = query.filter(Update.timestamp <= end_ts)
            
        query = query.order_by(desc(Update.timestamp))
        
        offers = query.limit(limit).all()
        
        result = []
        for offer in offers:
            result.append({
                "timestamp": offer.product_status.update.timestamp,
                "amount": offer.amount,
                "pricePerUnit": offer.pricePerUnit,
                "orders": offer.orders
            })
            
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/health")
def health_check():
    return {"status": "ok"}
