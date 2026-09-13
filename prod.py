import os
import time
import json
import requests
import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# Initialize the Kafka producer(external listener)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9094')
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "stock_intraday_msft") 

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
print(producer) 

# ---- API extraction with basic retry (Alpha Vantage / RapidAPI)----
RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("X_RAPIDAPI_HOST", "alpha-vantage.p.rapidapi.com")
INTERVAL      = os.getenv("INTERVAL", "5min")      
OUTPUT_SIZE   = os.getenv("OUTPUT_SIZE", "compact")

SYMBOLS_ENV = os.getenv("SYMBOL", "MSFT")
SYMBOLS = [s.strip().upper() for s in SYMBOLS_ENV.replace(" ", "").split(",") if s.strip()]

POLL_INTERVAL_SEC     = int(os.getenv("POLL_INTERVAL_SEC", "300"))   
PER_SYMBOL_DELAY_SEC  = int(os.getenv("PER_SYMBOL_DELAY_SEC", "5"))  

URL = "https://alpha-vantage.p.rapidapi.com/query"
HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
}

def get_stock_data(symbol: str):

    """Fetch intraday JSON for a single symbol with simple retry (3 attempts)."""

    querystring = {
        "datatype": "json",
        "output_size": OUTPUT_SIZE,
        "interval": INTERVAL,
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
    }
    for attempt in range(3):
        try:
            resp = requests.get(URL, headers=HEADERS, params=querystring, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            print(f"[{symbol}] HTTP {resp.status_code} attempt {attempt+1}/3")
        except Exception as e:
            print(f"[{symbol}] request error attempt {attempt+1}/3: {e}")
        if attempt < 2:
            time.sleep(60)  
    raise RuntimeError(f"[{symbol}] API request failed after 3 attempts")


def fetch_and_produce_stock_price():

    """Continuously fetch & produce Alpha Vantage intraday for multiple symbols."""

    print("Starting stock data producer...")
    print(f"Symbols: {SYMBOLS}")
    print("Press Ctrl+C to stop")

    while True:
        cycle_start = time.time()
        try:
            for sym in SYMBOLS:
                try:
                    # Fetch fresh data from API
                    stockdata = get_stock_data(sym)
                    stockdata["ingested_at"] = int(time.time())
                    stockdata["_symbol_hint"] = sym
                    stockdata["_interval"] = INTERVAL
                    serialized_data = json.dumps(stockdata).encode("utf-8")

                    # Produce data to Kafka (use produce, not send) key=symbol helps visibility in Kafka UI
                    producer.produce(KAFKA_TOPIC, value=serialized_data, key=sym.encode("utf-8"))
                    producer.poll(0)  # serve delivery callbacks
                    print(f"[{sym}] sent to {KAFKA_TOPIC} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    print(f"[{sym}] error: {e}")
 
                time.sleep(PER_SYMBOL_DELAY_SEC)

            producer.flush() # Ensure the message is sent immediately
            
        except KeyboardInterrupt:
            print("\nStopping producer...")
            break
        except Exception as e:
            print(f"[CYCLE] error: {e}")

        # sleep the remainder of the poll interval (align to your intraday)
        elapsed = time.time() - cycle_start
        sleep_for = max(0, POLL_INTERVAL_SEC - int(elapsed))
        print(f"Waiting {sleep_for} seconds before next fetch...")
        time.sleep(sleep_for)

if __name__ == "__main__":
    fetch_and_produce_stock_price()
