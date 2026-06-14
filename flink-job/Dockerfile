FROM --platform=linux/amd64 python:3.10-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && pip install --no-cache-dir apache-flink==1.18.1 \
    && rm -rf /var/lib/apt/lists/*

COPY models.py alert_engine.py error_rate_job.py ./

ENV EXECUTION_TARGET=cluster \
    KAFKA_BOOTSTRAP=kafka:9092 \
    THRESHOLDS_PATH=/opt/flink/config-custom/thresholds.json

ENTRYPOINT ["python", "error_rate_job.py"]
