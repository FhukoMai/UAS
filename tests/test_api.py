import time
import uuid
import requests
import pytest
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

def wait_until(condition_fn, timeout=10, interval=0.5):
    """Poll condition_fn every `interval` seconds until it returns True or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition_fn():
            return True
        time.sleep(interval)
    return False

BASE_URL = "http://localhost:8080"

@pytest.fixture(scope="module", autouse=True)
def wait_for_api():
    """Wait for the API to be ready before running tests."""
    for _ in range(30):
        try:
            res = requests.get(f"{BASE_URL}/health")
            if res.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    pytest.fail("API did not become ready")

def generate_event(topic="test_topic", event_id=None):
    if not event_id:
        event_id = str(uuid.uuid4())
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_script",
        "payload": {"test": True}
    }

def test_health_check():
    response = requests.get(f"{BASE_URL}/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_publish_empty_events():
    response = requests.post(f"{BASE_URL}/publish", json={"events": []})
    assert response.status_code == 200
    assert response.json()["status"] == "ignored"

def test_publish_invalid_schema():
    invalid_event = {"topic": "test", "payload": "missing_fields"}
    response = requests.post(f"{BASE_URL}/publish", json={"events": [invalid_event]})
    assert response.status_code == 422

def test_publish_invalid_timestamp():
    event = generate_event("invalid_time")
    event["timestamp"] = "not-a-timestamp"
    response = requests.post(f"{BASE_URL}/publish", json={"events": [event]})
    assert response.status_code == 422

def test_dedup_single_event():
    event_id = str(uuid.uuid4())
    topic = f"dedup_test_{uuid.uuid4().hex[:8]}"
    event = generate_event(topic, event_id)

    requests.post(f"{BASE_URL}/publish", json={"events": [event]})
    requests.post(f"{BASE_URL}/publish", json={"events": [event]})

    def event_inserted():
        resp = requests.get(f"{BASE_URL}/events?topic={topic}")
        events = resp.json().get("events", [])
        return any(e["event_id"] == event_id for e in events)
    assert wait_until(event_inserted, timeout=15), "Event was not inserted in time"

    time.sleep(1)
    events_resp = requests.get(f"{BASE_URL}/events?topic={topic}").json()
    count = sum(1 for e in events_resp["events"] if e["event_id"] == event_id)
    assert count == 1, f"Expected 1 unique event, got {count} (dedup failed)"

def test_concurrency_race_condition():
    event_id = str(uuid.uuid4())
    event = generate_event("concurrency_test", event_id)
    payload = {"events": [event]}
    
    def publish_task():
        return requests.post(f"{BASE_URL}/publish", json=payload)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(publish_task) for _ in range(10)]
        results = [f.result() for f in futures]
        
    for r in results:
        assert r.status_code == 200

    time.sleep(2)
    
    events_resp = requests.get(f"{BASE_URL}/events?topic=concurrency_test").json()
    count = sum(1 for e in events_resp["events"] if e["event_id"] == event_id)
    assert count == 1

def test_get_events_by_topic():
    event_id = str(uuid.uuid4())
    topic = f"topic_{event_id}"
    event = generate_event(topic, event_id)
    
    requests.post(f"{BASE_URL}/publish", json={"events": [event]})
    time.sleep(1)
    
    response = requests.get(f"{BASE_URL}/events?topic={topic}")
    assert response.status_code == 200
    data = response.json()
    assert data["topic"] == topic
    assert len(data["events"]) == 1
    assert data["events"][0]["event_id"] == event_id

def test_stats_consistency():
    topic = f"stats_test_{uuid.uuid4().hex[:8]}"
    unique_events = [generate_event(topic) for _ in range(5)]
    duplicates = unique_events.copy()
    batch = unique_events + duplicates

    initial_received = requests.get(f"{BASE_URL}/stats").json()["received"]
    requests.post(f"{BASE_URL}/publish", json={"events": batch})

    received_after = requests.get(f"{BASE_URL}/stats").json()["received"]
    assert received_after >= initial_received + 10

    def consistency_done():
        resp = requests.get(f"{BASE_URL}/events?topic={topic}")
        events = resp.json().get("events", [])
        return len(events) >= 5
    assert wait_until(consistency_done, timeout=20), "Events did not get stored in time"

    events_resp = requests.get(f"{BASE_URL}/events?topic={topic}").json()
    assert len(events_resp["events"]) == 5

def test_stress_small_batch():
    batch_size = 500
    events = [generate_event("stress_test") for _ in range(batch_size)]
    
    start_time = time.time()
    response = requests.post(f"{BASE_URL}/publish", json={"events": events})
    elapsed = time.time() - start_time
    
    assert response.status_code == 200
    assert elapsed < 2.0
    time.sleep(3)
    events_resp = requests.get(f"{BASE_URL}/events?topic=stress_test").json()
    assert len(events_resp["events"]) > 0

def test_get_stats_fields():
    response = requests.get(f"{BASE_URL}/stats")
    assert response.status_code == 200
    data = response.json()
    assert "received" in data
    assert "unique_processed" in data
    assert "duplicate_dropped" in data
    assert "topics" in data
    assert "uptime_seconds" in data
    assert isinstance(data["uptime_seconds"], float)

def test_persistence_simulated():
    response = requests.get(f"{BASE_URL}/stats")
    data = response.json()
    assert data["received"] > 0
    assert data["unique_processed"] > 0

def test_malformed_json():
    headers = {'Content-Type': 'application/json'}
    response = requests.post(f"{BASE_URL}/publish", data="{invalid json}", headers=headers)
    assert response.status_code == 422

def test_missing_topic():
    response = requests.get(f"{BASE_URL}/events")
    assert response.status_code == 422

def test_stats_topics_includes_new():
    event = generate_event("brand_new_topic_123")
    requests.post(f"{BASE_URL}/publish", json={"events": [event]})

    def topic_visible():
        resp = requests.get(f"{BASE_URL}/stats")
        return "brand_new_topic_123" in resp.json().get("topics", [])
    assert wait_until(topic_visible, timeout=15), "New topic did not appear in stats in time"

    response = requests.get(f"{BASE_URL}/stats")
    assert "brand_new_topic_123" in response.json()["topics"]
