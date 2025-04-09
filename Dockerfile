FROM python:3.11-slim

ENV OLLAMA_HOST \
    OLLAMA_PORT \
    KAFKA_HOST \
    KAFKA_PORT \
    DOCUHELPER_FILE_ENDPOINT

COPY . /app

WORKDIR app

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "Main.py"]