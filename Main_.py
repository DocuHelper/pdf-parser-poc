import json
from datetime import datetime
import requests
import pymupdf4llm
from confluent_kafka import Consumer, KafkaException, Producer
from ollama import Client
from langchain.text_splitter import RecursiveCharacterTextSplitter

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

    # 신규
    md_text = pymupdf4llm.to_markdown(
        doc=file_path,
        page_chunks=True,
    )

    # 전체 텍스트 연결
    all_text = "\n\n".join([page["text"] for page in md_text])

    # 페이지 시작 인덱스 기록
    page_start_indices = []
    offset = 0
    for page in md_text:
        page_start_indices.append(offset)
        offset += len(page["text"]) + 2  # "\n\n" 길이 고려

    # 전체 텍스트 청킹
    splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=150)
    full_chunks = splitter.split_text(all_text)

    from bisect import bisect_right

    # 청크별 포함된 페이지 추정 및 출력
    for i, chunk in enumerate(full_chunks):
        start_index = all_text.find(chunk[:30])
        page_idx = bisect_right(page_start_indices, start_index) - 1
        if start_index == -1 or page_idx < 0:
            page_num = -1
        else:
            page_num = page_idx + 1
        print(
            f"======================================== [FullDoc Chunk {i + 1} | Page {page_num}] ======================================== \n{chunk}\n")
        pageEmbdRes = ollama.embeddings(model='bge-m3', prompt=chunk).embedding

        document_parse_event = {
            "documentUuid": document_uuid,
            "page": page_num,
            "chunkNum": i + 1,
            "content": chunk,
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
