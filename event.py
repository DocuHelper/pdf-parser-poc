import json
from datetime import datetime
import os
from confluent_kafka import Producer

KAFKA_HOST = os.environ.get("KAFKA_HOST")
KAFKA_PORT = os.environ.get("KAFKA_PORT")

producer_config = {
    'bootstrap.servers': f"{KAFKA_HOST}:{KAFKA_PORT}"
}
producer = Producer(producer_config)


def publish_event(topic, key, value):
    event_key = {
        "eventType": key,
        "eventPublishDt": list(datetime.utcnow().timetuple()[:6]) + [int(datetime.utcnow().microsecond * 1000)]
    }

    headers = [
        ("eventType", key.encode("utf-8")),
    ]

    print(f"Publish Event : {event_key}")

    producer.produce(
        topic=topic,
        key=json.dumps(event_key),
        headers=headers,
        value=json.dumps(value),
        callback=lambda err, msg: print("전송 성공" if err is None else f"에러 발생: {err}")
    )
