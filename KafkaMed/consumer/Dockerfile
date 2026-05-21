FROM python:3.11-slim

WORKDIR /app

# Java es requerido por PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pymongo==4.7.3 \
    numpy==1.26.4

COPY consumer.py .

CMD ["python", "consumer.py"]
