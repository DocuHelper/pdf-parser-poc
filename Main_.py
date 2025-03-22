import json
from datetime import datetime
import requests
import pymupdf4llm
from confluent_kafka import Consumer, KafkaException, Producer
from ollama import Client

ollama = Client(host='http://192.168.0.8:11434')

producer_config = {
    'bootstrap.servers': '192.168.0.7:9092'  # Kafka 브로커 주소
}
producer = Producer(producer_config)

consumer_config = {
    'bootstrap.servers': '192.168.0.7:9092',
    'group.id': 'new_test_consumer_group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(consumer_config)
consumerTopic = 'docuhelper-api'

consumer.subscribe([consumerTopic])


def publishEvent(topic, key, value):
    event_key = {
        "eventType": key,
        "eventPublishDt": list(datetime.utcnow().timetuple()[:6]) + [int(datetime.utcnow().microsecond * 1000)]
    }

    print(f"Publish Event : {event_key}")

    producer.produce(
        topic=topic,
        key=json.dumps(event_key),
        value=json.dumps(value),
        callback=lambda err, msg: print("전송 성공" if err is None else f"에러 발생: {err}")
    )


def pdfParse(document_uuid, document_file_uuid, file_path):
    document_parse_start_event = {
        "documentUuid": document_uuid
    }

    publishEvent("docuhelper-document-parser", "DocumentParseStart", document_parse_start_event)

    md_text = pymupdf4llm.to_markdown(
        doc=file_path,
        page_chunks=True,
        # write_images=True,
    )
    for current in md_text:
        page = current['metadata']['page']
        pageContent = current["text"]
        pageEmbdRes = ollama.embeddings(model='bge-m3', prompt=pageContent).embedding

        document_parse_event = {
            "documentUuid": document_uuid,
            "page": page,
            "content": pageContent,
            "embedContent": pageEmbdRes
        }

        publishEvent("docuhelper-document-parser", "DocumentParse", document_parse_event)

    document_parse_complete_event = {
        "documentUuid": document_uuid
    }

    publishEvent("docuhelper-document-parser", "DocumentParseComplete", document_parse_complete_event)


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
        file_url = f"http://localhost:8082/file/{document_file_uuid}"
        print(f"Sending request to: {file_url}")
        # HTTP 요청 전송
        response = requests.get(file_url)
        if response.status_code != 200:
            print(f"File request failed with status {response.status_code}: {file_url}")
            continue
        print(f"File request successful: {file_url}")
        file_link = response.text.strip().strip('"')
        print(f"File Download Link: {file_link}")
        # 파일 다운로드
        response = requests.get(file_link)
        if response.status_code != 200:
            print(f"Failed to download file from link: {file_link}")
            continue
        file_path = f"./pdf/{document_file_uuid}.pdf"
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded: {file_path}")

        pdfParse(document_uuid, document_file_uuid, file_path)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
