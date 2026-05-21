"""
KafkaMed · Consumer (Spark Structured Streaming)
Lee mensajes del topic 'heart-records', aplica el modelo Random Forest
entrenado en Colab y almacena cada predicción en MongoDB.
"""

import os
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, current_timestamp, round as spark_round
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType
)
from pyspark.ml import PipelineModel
from pymongo import MongoClient

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI         = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MODEL_PATH        = os.getenv("MODEL_PATH", "/app/datos/modelo_cardiaco")
TOPIC             = "heart-records"

# ------------------------------------------------------------------
# Esquema del mensaje JSON que llega desde el producer
# ------------------------------------------------------------------
SCHEMA = StructType([
    StructField("Age",            IntegerType()),
    StructField("Sex",            StringType()),
    StructField("ChestPainType",  StringType()),
    StructField("RestingBP",      IntegerType()),
    StructField("Cholesterol",    IntegerType()),
    StructField("FastingBS",      IntegerType()),
    StructField("RestingECG",     StringType()),
    StructField("MaxHR",          IntegerType()),
    StructField("ExerciseAngina", StringType()),
    StructField("Oldpeak",        DoubleType()),
    StructField("ST_Slope",       StringType()),
    StructField("HeartDisease",   IntegerType()),
])


def crear_spark():
    return (
        SparkSession.builder
        .appName("KafkaMed-Consumer")
        .master("local[2]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def guardar_en_mongo(df, epoch_id):
    """Función que se llama por cada micro-lote del stream."""
    registros = df.collect()
    if not registros:
        return

    client = MongoClient(MONGO_URI)
    col_mongo = client["kafkamed"]["predicciones"]

    docs = []
    for row in registros:
        docs.append({
            "Age":                row["Age"],
            "Sex":                row["Sex"],
            "ChestPainType":      row["ChestPainType"],
            "RestingBP":          row["RestingBP"],
            "Cholesterol":        row["Cholesterol"],
            "FastingBS":          row["FastingBS"],
            "RestingECG":         row["RestingECG"],
            "MaxHR":              row["MaxHR"],
            "ExerciseAngina":     row["ExerciseAngina"],
            "Oldpeak":            row["Oldpeak"],
            "ST_Slope":           row["ST_Slope"],
            "etiqueta_real":      row["HeartDisease"],
            "prediccion":         row["prediccion"],
            "probabilidad_riesgo": row["probabilidad_riesgo"],
            "timestamp":          datetime.utcnow().isoformat(),
        })

    col_mongo.insert_many(docs)
    client.close()
    print(f"✅ Lote {epoch_id}: {len(docs)} predicciones guardadas en MongoDB")


if __name__ == "__main__":
    print("🚀 KafkaMed Consumer iniciando...")

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Cargar modelo entrenado en Colab
    print(f"⏳ Cargando modelo desde {MODEL_PATH}...")
    model = PipelineModel.load(MODEL_PATH)
    print("✅ Modelo cargado")

    # UDFs para transformar la predicción
    etiqueta_udf    = udf(lambda p: "riesgo" if int(p) == 1 else "sin_riesgo", StringType())
    prob_riesgo_udf = udf(lambda v: round(float(v[1]), 4), FloatType())

    # Leer stream desde Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Deserializar JSON
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), SCHEMA).alias("data"))
        .select("data.*")
    )

    # Aplicar modelo
    predictions = model.transform(parsed)

    # Formatear salida
    output = (
        predictions
        .withColumn("prediccion", etiqueta_udf(col("prediction")))
        .withColumn("probabilidad_riesgo", prob_riesgo_udf(col("probability")))
        .select(
            "Age", "Sex", "ChestPainType", "RestingBP", "Cholesterol",
            "FastingBS", "RestingECG", "MaxHR", "ExerciseAngina",
            "Oldpeak", "ST_Slope", "HeartDisease",
            "prediccion", "probabilidad_riesgo"
        )
    )

    # Escribir a MongoDB por micro-lotes
    query = (
        output.writeStream
        .foreachBatch(guardar_en_mongo)
        .option("checkpointLocation", "/tmp/kafkamed_checkpoint")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("✅ Stream activo — esperando mensajes de Kafka...")
    query.awaitTermination()
