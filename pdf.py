from event import publish_event
import pymupdf4llm
from langchain.text_splitter import RecursiveCharacterTextSplitter
from ollama import Client
import os
from unittest.mock import patch

OLLAMA_HOST = os.environ.get("OLLAMA_HOST")
OLLAMA_PORT = os.environ.get("OLLAMA_PORT")
ollama = Client(host=f"http://{OLLAMA_HOST}:{OLLAMA_PORT}")

def pdf_parse(document_uuid, file_path):
    document_parse_start_event = {
        "documentUuid": document_uuid
    }

    publish_event("docuhelper-document-parser", "DocumentParseStart", document_parse_start_event)

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
        # print(
        #     f"======================================== [FullDoc Chunk {i + 1} | Page {page_num}] ======================================== \n{chunk}\n")
        pageEmbdRes = ollama.embeddings(model='bge-m3', prompt=chunk).embedding

        document_parse_event = {
            "documentUuid": document_uuid,
            "page": page_num,
            "chunkNum": i + 1,
            "content": chunk,
            "embedContent": pageEmbdRes
        }

        publish_event("docuhelper-document-parser", "DocumentParse", document_parse_event)

    document_parse_complete_event = {
        "documentUuid": document_uuid
    }

    publish_event("docuhelper-document-parser", "DocumentParseComplete", document_parse_complete_event)


def test_pdf_parse_without_event_publish(dummy_file_path):
    with patch("__main__.publish_event") as mock_publish:
        pdf_parse("dummy_document_uuid", dummy_file_path)

if __name__ == '__main__':
    test_pdf_parse_without_event_publish("./노동위원회법(법률)(제18179호)(20220519).pdf")
