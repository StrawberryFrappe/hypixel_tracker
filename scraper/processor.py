import os
import time
import logging
from pymongo import MongoClient
from pymongo import DESCENDING
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")

def get_db_connection():
    client = MongoClient(MONGO_URI)
    return client.hypixel_bazaar

def main():
    logging.info("Starting Processor Service...")
    db = get_db_connection()
    raw_collection = db.bazaar_raw
    bazaar_collection = db.bazaar
    
    while True:
        # Find the latest raw record
        latest_raw = raw_collection.find_one(sort=[("data.lastUpdated", DESCENDING)])
        
        if latest_raw:
            raw_last_updated = latest_raw.get("data", {}).get("lastUpdated")
            
            # Check if this lastUpdated is already in bazaar collection
            # We assume bazaar collection documents have 'lastUpdated' field at root or inside products?
            # Let's store 'lastUpdated' at root of processed record for easy querying.
            
            latest_processed = bazaar_collection.find_one(sort=[("lastUpdated", DESCENDING)])
            
            should_process = True
            if latest_processed:
                processed_last_updated = latest_processed.get("lastUpdated")
                if processed_last_updated == raw_last_updated:
                    should_process = False
            
            if should_process:
                logging.info(f"Processing new data. lastUpdated: {raw_last_updated}")
                
                products = latest_raw.get("data", {}).get("products", {})
                
                processed_record = {
                    "timestamp": datetime.utcnow(), # Processing time
                    "lastUpdated": raw_last_updated, # Source time
                    "products": products
                }
                
                try:
                    bazaar_collection.insert_one(processed_record)
                    logging.info(f"Processed data stored. {len(products)} products.")
                except Exception as e:
                    logging.error(f"Error storing processed data: {e}")
            else:
                # logging.info("No new data to process.")
                pass
        
        time.sleep(10) # Check every 10 seconds

if __name__ == "__main__":
    main()
