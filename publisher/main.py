import os
import time
import json
import uuid
import random
import logging
from datetime import datetime, timezone
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - publisher - %(levelname)s - %(message)s')
logger = logging.getLogger("publisher")

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1.0"))

TOPICS = ["user_events", "system_metrics", "audit_logs"]
SOURCES = ["web_client_1", "mobile_app_1", "backend_service_1"]

def generate_events(batch_size=10, duplicate_ratio=0.3):
    events = []
    num_duplicates = int(batch_size * duplicate_ratio)
    num_unique = batch_size - num_duplicates
    
    unique_events = []
    for _ in range(num_unique):
        event = {
            "topic": random.choice(TOPICS),
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": random.choice(SOURCES),
            "payload": {
                "metric": random.randint(1, 100),
                "action": "generated"
            }
        }
        unique_events.append(event)
        events.append(event)
        
    for _ in range(num_duplicates):
        if unique_events:
            events.append(random.choice(unique_events))
            
    random.shuffle(events)
    return events

def main():
    logger.info(f"Starting publisher. Target URL: {TARGET_URL}, Interval: {PUBLISH_INTERVAL}s")
    
    for _ in range(30):
        try:
            health_res = requests.get(TARGET_URL.replace("/publish", "/health"))
            if health_res.status_code == 200:
                logger.info("Aggregator is ready!")
                break
        except requests.exceptions.ConnectionError:
            pass
        logger.info("Waiting for aggregator...")
        time.sleep(2)
        
    while True:
        try:
            events = generate_events(batch_size=50, duplicate_ratio=0.3)
            
            payload = {"events": events}
            start_time = time.time()
            response = requests.post(TARGET_URL, json=payload, timeout=5)
            latency = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                logger.info(f"Published batch of {len(events)} events in {latency:.2f}ms. Response: {response.json()}")
            else:
                logger.error(f"Failed to publish: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error publishing events: {e}")
            
        time.sleep(PUBLISH_INTERVAL)

if __name__ == "__main__":
    main()
