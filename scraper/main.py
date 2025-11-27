import os
import time
import requests
import logging
from pymongo import MongoClient
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
API_URL = "https://api.hypixel.net/v2/skyblock/bazaar"

def get_db_connection():
    client = MongoClient(MONGO_URI)
    return client.hypixel_bazaar

def fetch_bazaar_data():
    headers = {}
    
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None

def process_and_store(db, data):
    if not data or not data.get("success"):
        logging.warning("Invalid data received or success is False")
        return

    timestamp = datetime.utcnow()
    
    # 1. Store Raw Data
    raw_collection = db.bazaar_raw
    raw_record = {
        "timestamp": timestamp,
        "data": data
    }
    try:
        raw_collection.insert_one(raw_record)
        logging.info("Raw data stored successfully.")
    except Exception as e:
        logging.error(f"Error storing raw data: {e}")

    # 2. Process and Store Cleaned Data
    # The Bazaar data is in data['products']
    products = data.get("products", {})
    
    # We will store the entire snapshot of products with a timestamp
    # Flattening isn't strictly necessary if we just want the products dict, 
    # but let's ensure we just keep the relevant part.
    
    processed_record = {
        "timestamp": timestamp,
        "products": products
    }
    
    # Upsert into 'bazaar' collection. 
    # We might want to keep history or just the latest. 
    # The requirement says "Upserts the data into MongoDB with a timestamp."
    # If we want a history of processed data, we just insert. 
    # If we want "latest" state, we might upsert based on a single ID.
    # However, for a tracker, usually history is good. 
    # But the API service needs "latest".
    # Let's insert for history, and we can query sort by timestamp desc for latest.
    
    collection = db.bazaar
    try:
        collection.insert_one(processed_record)
        logging.info(f"Processed data stored successfully. {len(products)} products.")
    except Exception as e:
        logging.error(f"Error storing processed data: {e}")

def main():
    logging.info("Starting Scraper Service...")
    db = get_db_connection()
    
    while True:
        start_time = time.time()
        
        data = fetch_bazaar_data()
        if data:
            process_and_store(db, data)
        
        # Sleep logic
        elapsed = time.time() - start_time
        sleep_time = max(15 - elapsed, 1) # Ensure at least 1 sec sleep, target 15s loop
        logging.info(f"Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
