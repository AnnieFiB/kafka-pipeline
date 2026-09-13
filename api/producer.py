import os
import time
import json
import requests
import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9094')
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "stock_intraday_msft")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# Alpha Vantage API configuration
RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("X_RAPIDAPI_HOST", "alpha-vantage.p.rapidapi.com")
OUTPUT_SIZE   = os.getenv("OUTPUT_SIZE", "compact")
SYMBOLS_ENV   = os.getenv("SYMBOL", "MSFT")
SYMBOLS       = [s.strip().upper() for s in SYMBOLS_ENV.replace(" ", "").split(",") if s.strip()]
PER_SYMBOL_DELAY_SEC = int(os.getenv("PER_SYMBOL_DELAY_SEC", "5"))

# API endpoint and headers
URL = "https://alpha-vantage.p.rapidapi.com/query"
HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
}

# Fetch daily stock data for a symbol
def get_stock_data(symbol: str):
    querystring = {
        "datatype": "json",
        "output_size": OUTPUT_SIZE,
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

# Wait until next midnight
def wait_until_midnight():
    now = datetime.datetime.now()
    next_midnight = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    sleep_seconds = (next_midnight - now).total_seconds()
    print(f"Waiting {int(sleep_seconds)} seconds until next 12 AM...")
    time.sleep(sleep_seconds)

# Main producer loop
def fetch_and_produce_stock_price():
    print("Starting daily stock data producer...")
    print(f"Symbols: {SYMBOLS}")
    print("Press Ctrl+C to stop")

    while True:
        try:
            for sym in SYMBOLS:
                try:
                    stockdata = get_stock_data(sym)
                    stockdata["ingested_at"] = int(time.time())
                    stockdata["_symbol_hint"] = sym
                    stockdata["_interval"] = "daily"
                    serialized_data = json.dumps(stockdata).encode("utf-8")
                    producer.produce(KAFKA_TOPIC, value=serialized_data, key=sym.encode("utf-8"))
                    producer.poll(0)
                    print(f"[{sym}] sent to {KAFKA_TOPIC} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    print(f"[{sym}] error: {e}")
                time.sleep(PER_SYMBOL_DELAY_SEC)

            producer.flush()
        except KeyboardInterrupt:
            print("\nStopping producer...")
            break
        except Exception as e:
            print(f"[CYCLE] error: {e}")

        wait_until_midnight()

if __name__ == "__main__":
    fetch_and_produce_stock_price()
