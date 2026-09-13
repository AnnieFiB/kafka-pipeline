import os, json, time
from fastapi import FastAPI
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
producer = Producer({"bootstrap.servers": BOOTSTRAP})

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/event")
def event(payload: dict):
    payload["ingested_at"] = int(time.time())
    producer.produce(TOPIC, json.dumps(payload).encode("utf-8"))
    producer.flush()
    return {"status": "queued"}

