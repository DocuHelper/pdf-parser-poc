import json
import os
from confluent_kafka import Consumer, KafkaException, Producer

from event import publish_event
from file import get_file_download_url, download_file
from pdf import pdf_parse

KAFKA_HOST = os.environ.get("KAFKA_HOST")
KAFKA_PORT = os.environ.get("KAFKA_PORT")

os.makedirs("./pdf", exist_ok=True)

producer_config = {
    'bootstrap.servers': f"{KAFKA_HOST}:{KAFKA_PORT}"
}
producer = Producer(producer_config)

consumer_config = {
    'bootstrap.servers': f"{KAFKA_HOST}:{KAFKA_PORT}",
    'group.id': 'docuhelper-pdf-parser',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(consumer_config)
consumerTopic = 'docuhelper-api'

consumer.subscribe([consumerTopic])


def getEventValue(msg):
    message_value = msg.value().decode('utf-8')
    print(f"Received Event Value: {message_value}")
    message_json = json.loads(message_value)
    return message_json


def getEventKey(msg):
    key = msg.key().decode('utf-8')
    print(f"Received Event Key: {key}")
    key_json = json.loads(key)
    return key_json


try:
    print(f"Consuming messages from topic: {consumerTopic}")
    while True:
        msg = consumer.poll(timeout=0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        headers = msg.headers()
        header_dict = {k: v.decode('utf-8') if v else None for k, v in headers}
        event_type = header_dict.get('eventType')

        if event_type != "DocumentCreate":
            continue

        key_json = None
        message_json = None

        try:
            key_json = getEventKey(msg)
            message_json = getEventValue(msg)
        except json.JSONDecodeError:
            print("Failed to parse JSON message.")
            continue

        document_uuid = message_json.get("document", {}).get("uuid")
        document_file_uuid = message_json.get("document", {}).get("file")
        try:
            document_file_download_url = get_file_download_url(document_file_uuid)
            download_file_path = download_file(document_file_download_url, document_file_uuid)

            pdf_parse(document_uuid, download_file_path)
        except:
            document_parse_fail_event = {"documentUuid": document_uuid}
            publish_event("docuhelper-document-parser", "DocumentParseFail", document_parse_fail_event)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
