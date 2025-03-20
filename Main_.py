import json
import requests
import pathlib
import pymupdf4llm
from confluent_kafka import Consumer, KafkaException
from pprint import pprint


def pdfToMarkDown(uuid,file_path):
    md_text = pymupdf4llm.to_markdown(
        doc=file_path,
        write_images=True
    )
    # Markdown 내용 출력 및 저장
    md_output_path = f"{uuid}.md"
    pathlib.Path(md_output_path).write_text(md_text, encoding="utf-8")
    print(f"Markdown extracted and saved: {md_output_path}")
    print(md_text)

def pdfParse(uuid,file_path):
    md_text = pymupdf4llm.to_markdown(
        doc=file_path,
        write_images=True,
        page_chunks=True
    )
    for current in md_text:
        pprint(current)
        print("==========================================")


consumer_config = {
    'bootstrap.servers': '192.168.0.7:9092',
    'group.id': 'new_test_consumer_group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(consumer_config)
topic = 'docuhelper-api'

consumer.subscribe([topic])

try:
    print(f"Consuming messages from topic: {topic}")
    while True:
        msg = consumer.poll(timeout=0)  # 메시지를 1초 동안 기다림
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        message_value = msg.value().decode('utf-8')
        print(f"Received message: {message_value}")
        message_json = None
        try:
            message_json = json.loads(message_value)
        except json.JSONDecodeError:
            print("Failed to parse JSON message.")
            continue

        uuid = message_json.get("document", {}).get("file")
        file_url = f"http://localhost:8082/file/{uuid}"
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
        file_path = f"{uuid}.pdf"
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded: {file_path}")
        # pdfToMarkDown(uuid, file_path)
        pdfParse(uuid, file_path)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()


