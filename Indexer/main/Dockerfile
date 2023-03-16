FROM python:slim-buster

WORKDIR /container/indexer
COPY . /container/indexer

RUN pip install --no-cache-dir -r requirements.txt

RUN python3 setup.py


CMD ["python3", "live_updater.py"]

