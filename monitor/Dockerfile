FROM python:3.8
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY monitor.py .
ENTRYPOINT ["fastkafka", "run", "--num-workers", "1", "--kafka-broker", "compose", "monitor:kafka_app"]