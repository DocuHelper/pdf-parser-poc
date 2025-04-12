import requests
import os

DOCUHELPER_FILE_ENDPOINT = os.environ.get("DOCUHELPER_FILE_ENDPOINT")

def get_file_download_url(document_file_uuid):
    file_url = f"{DOCUHELPER_FILE_ENDPOINT}/file/{document_file_uuid}"
    print(f"Sending request to: {file_url}")
    response = requests.get(file_url)
    if response.status_code != 200:
        raise Exception(f"File request failed with status {response.status_code}: {file_url}")
    print(f"File request successful: {file_url}")
    file_link = response.text.strip().strip('"')
    print(f"File Download Link: {file_link}")
    return file_link

def download_file(download_url, fileName):
    response = requests.get(download_url)
    if response.status_code != 200:
        raise Exception(f"File request failed with status {response.status_code}: {download_url}")
    file_path = f"./pdf/{fileName}.pdf"
    with open(file_path, "wb") as file:
        file.write(response.content)
    print(f"File downloaded: {file_path}")
    return file_path