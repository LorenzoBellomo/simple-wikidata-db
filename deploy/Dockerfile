FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    python3.9 \
    python3-pip\
    aria2

RUN python3 -m pip install --upgrade pip

COPY requirements.txt .

ENV PYTHONUNBUFFERED=0

RUN pip --no-cache-dir install -r requirements.txt

RUN aria2c --max-connection-per-server 16 https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz

RUN python3 preprocess_dump.py \ 
    --input_file ./latest-all.json.gz \
    --out_dir ./results \
    --language_id en

RUN python3 fetch_bio_entities.py


