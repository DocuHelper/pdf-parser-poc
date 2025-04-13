import os
import ssl
import certifi
import pathlib
from unittest.mock import patch
import nltk

from langchain.text_splitter import RecursiveCharacterTextSplitter
from unstructured.partition.pdf import partition_pdf
from ollama import Client
from event import publish_event


ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
nltk.download("averaged_perceptron_tagger_eng")
nltk.download('punkt_tab')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

OLLAMA_HOST = os.environ.get("OLLAMA_HOST")
OLLAMA_PORT = os.environ.get("OLLAMA_PORT")

ollama = Client(host=f"http://{OLLAMA_HOST}:{OLLAMA_PORT}")

def pdf_parse(document_uuid, file_path):
    document_parse_start_event = {
        "documentUuid": document_uuid
    }

    publish_event("docuhelper-document-parser", "DocumentParseStart", document_parse_start_event)

    os.makedirs(f"./pdf/{document_uuid}", exist_ok=True)

    elements = partition_pdf(
        filename=file_path,
        strategy='hi_res',
        extract_images_in_pdf=True,
        infer_table_structure=True,
        languages='kor'  # ✅ 여기만 수정
    )

    all_text = "\n\n".join([el.text for el in elements if hasattr(el, 'text') and el.text])

    # image_paths = [el.metadata.image_path for el in elements if hasattr(el.metadata, 'image_path')]
    # print("추출된 이미지 경로들:")
    # for path in image_paths:
    #     print(path)

    pathlib.Path("output.md").write_bytes(all_text.encode())

    page_map = {}
    offset = 0
    for el in elements:
        text = getattr(el, "text", "")
        if not text:
            continue
        page = getattr(el.metadata, "page_number", None)
        if page not in page_map:
            page_map[page] = offset
        offset += len(text) + 2

    splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=150)
    full_chunks = splitter.split_text(all_text)

    for i, chunk in enumerate(full_chunks):
        start_index = all_text.find(chunk[:30])
        page_num = -1
        for page, idx in page_map.items():
            if start_index >= idx:
                page_num = page
            else:
                break

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
    with patch("__main__.publish_event") as mock_publish, \
         patch("__main__.ollama.embeddings") as mock_embeddings:
        mock_embeddings.return_value.embedding = [0.0] * 768
        pdf_parse("dummy_document_uuid", dummy_file_path)

if __name__ == '__main__':
    test_pdf_parse_without_event_publish("test_document.pdf")
    # test_pdf_parse_without_event_publish("노동위원회법(법률)(제18179호)(20220519).pdf")
