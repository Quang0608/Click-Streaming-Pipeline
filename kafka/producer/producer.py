from kafka import KafkaProducer
import json, time, uuid, random, datetime

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_tracking_event():
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    uid = str(uuid.uuid1())  # create_time as UUIDv1 based on timestamp

    event = {
        "create_time": uid,
        "bid": f"{random.uniform(1.0, 5.0):.1f}",
        "bn": random.choice(["Chrome 125", "Firefox 119", "Edge 118"]),
        "campaign_id": random.choice(["CAMP001", "CAMP002", "CAMP003", "CAMP004"]),
        "cd": random.choice(["CD123", "CD210", "CD250"]),
        "custom_track": random.choice(["click", "conversion", "qualified", "unqualified"]),
        "de": "UTF-8",
        "dl": random.choice([
            "http://example.com/jobs/designer",
            "http://example.com/jobs/engineer",
            "http://example.com/jobs/analyst"
        ]),
        "dt": random.choice(["JobPortal", "CareerPortal", "CandidatePortal"]),
        "ev": "2.0",
        "group_id": random.choice(["G01", "G02", "G03", "G04"]),
        "id": f"ID-{random.randint(1000, 9999)}",
        "job_id": f"J{random.randint(2000, 3000)}",
        "md": "True",
        "publisher_id": random.choice(["PUB001", "PUB002", "PUB003", "PUB004"]),
        "rl": random.choice([
            "http://linkedin.com",
            "http://facebook.com",
            "http://google.com"
        ]),
        "sr": random.choice(["1920x1080", "1366x768", "1600x900"]),
        "ts": ts,
        "tz": "+0800",
        "ua": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        ]),
        "uid": f"UID-{random.randint(1000, 9999)}",
        "utm_campaign": random.choice(["WinterDeals", "SummerSale", "CareerWeek"]),
        "utm_content": random.choice(["banner1", "banner2", "banner3", "popup1"]),
        "utm_medium": random.choice(["email", "social", "ad"]),
        "utm_source": random.choice(["linkedin", "facebook", "instagram"]),
        "utm_term": random.choice(["design", "career", "analytics"]),
        "v": "1.0",
        "vp": random.choice(["1920x1080", "1600x900"])
    }

    return event


# 🌀 Continuously send simulated events
if __name__ == "__main__":
    print("Producing tracking_new events to Kafka topic: tracking_events")
    while True:
        event = generate_tracking_event()
        producer.send("tracking_events", event)
        print(f"Sent: {json.dumps(event, indent=2)}")
        time.sleep(5)
