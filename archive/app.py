import time
import os
import json
from fastapi import FastAPI, HTTPException
from extract import get_stock_data, producer

app = FastAPI(title="Stock Market Data API", version="1.0.0")

@app.get("/health")
def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "stock-market-api"}

@app.get("/status")
def status():
    """Get current service status"""
    return {
        "service": "stock-market-api",
        "kafka_bootstrap": os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        "rapidapi_configured": bool(os.getenv('RAPIDAPI_KEY')),
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }

@app.post("/collect-data")
def collect_single_data():
    """Manually trigger a single data collection and send to Kafka"""
    try:
        # Fetch fresh data from API
        stockdata = get_stock_data()
        stockdata["ingested_at"] = int(time.time())
        serialized_data = json.dumps(stockdata).encode("utf-8")
        
        # Send to Kafka
        producer.produce('stock_intraday_msft', value=serialized_data)
        producer.flush()
        
        return {
            "status": "success", 
            "message": "Data collected and sent to Kafka",
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "data_points": len(stockdata.get('Time Series (5min)', {}))
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to collect data: {str(e)}")

@app.post("/event")
def event():
    """Legacy endpoint - triggers single data collection"""
    return collect_single_data()

@app.get("/")
def root():
    """API root endpoint with usage information"""
    return {
        "message": "Stock Market Data API",
        "endpoints": {
            "GET /health": "Health check",
            "GET /status": "Service status",
            "POST /collect-data": "Manually collect and send data to Kafka",
            "POST /event": "Legacy endpoint (same as /collect-data)"
        },
        "background_service": "stock-producer runs automatically every 5 minutes"
    }
