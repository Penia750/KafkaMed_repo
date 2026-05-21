"""
KafkaMed · Producer
Lee heart.csv fila a fila y publica cada registro como JSON
en el topic 'heart-records' de Kafka, simulando ingreso en tiempo real.
"""

import csv
import json
import os
import time

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATASET_PATH      = os.getenv("DATASET_PATH", "/app/datos/heart.csv")
INTERVAL_SECONDS  = float(os.getenv("INTERVAL_SECONDS", "2"))
TOPIC             = "heart-records"


def conectar_producer(retries=15, delay=5):
    """Reintenta la conexión a Kafka hasta que esté disponible."""
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"✅ Conectado a Kafka en {BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            print(f"⏳ Intento {i+1}/{retries} — Kafka no disponible: {e}")
            time.sleep(delay)
    raise RuntimeError("❌ No se pudo conectar a Kafka después de varios intentos")


def publicar_registros(producer):
    with open(DATASET_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        total = 0
        for row in reader:
            # Convertir tipos numéricos
            mensaje = {
                "Age":            int(row["Age"]),
                "Sex":            row["Sex"],
                "ChestPainType":  row["ChestPainType"],
                "RestingBP":      int(row["RestingBP"]),
                "Cholesterol":    int(row["Cholesterol"]),
                "FastingBS":      int(row["FastingBS"]),
                "RestingECG":     row["RestingECG"],
                "MaxHR":          int(row["MaxHR"]),
                "ExerciseAngina": row["ExerciseAngina"],
                "Oldpeak":        float(row["Oldpeak"]),
                "ST_Slope":       row["ST_Slope"],
                "HeartDisease":   int(row["HeartDisease"]),
            }
            producer.send(TOPIC, value=mensaje)
            total += 1
            print(f"📤 [{total}] Publicado: Age={mensaje['Age']} | "
                  f"Sex={mensaje['Sex']} | HeartDisease={mensaje['HeartDisease']}")
            time.sleep(INTERVAL_SECONDS)

        producer.flush()
        print(f"\n✅ {total} registros publicados en el topic '{TOPIC}'")


if __name__ == "__main__":
    print("🚀 KafkaMed Producer iniciando...")
    producer = conectar_producer()
    publicar_registros(producer)
