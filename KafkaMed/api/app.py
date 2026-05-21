"""
KafkaMed · API REST Flask
4 endpoints requeridos por el enunciado:
  GET /patients       → lista de pacientes procesados
  GET /predictions    → predicciones con filtros opcionales
  GET /stats          → métricas agregadas del modelo
  GET /risk-summary   → resumen de pacientes de alto riesgo
"""

from flask import Flask, jsonify, request
from pymongo import MongoClient
from datetime import datetime
import os

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
client    = MongoClient(MONGO_URI)
db        = client["kafkamed"]
col       = db["predicciones"]


# ------------------------------------------------------------------
# GET /patients
# Lista todos los pacientes procesados (sin duplicar campos internos)
# Params: ?limit=N (default 100)
# ------------------------------------------------------------------
@app.route("/patients", methods=["GET"])
def get_patients():
    limit = int(request.args.get("limit", 100))
    docs = list(col.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))
    return jsonify({"total": len(docs), "data": docs}), 200


# ------------------------------------------------------------------
# GET /predictions
# Predicciones con filtro opcional por clase
# Params: ?prediccion=riesgo|sin_riesgo  ?limit=N
# ------------------------------------------------------------------
@app.route("/predictions", methods=["GET"])
def get_predictions():
    prediccion = request.args.get("prediccion")
    limit      = int(request.args.get("limit", 100))

    filtro = {}
    if prediccion:
        filtro["prediccion"] = prediccion

    docs = list(
        col.find(filtro, {"_id": 0,
                          "prediccion": 1,
                          "probabilidad_riesgo": 1,
                          "Age": 1, "Sex": 1,
                          "ChestPainType": 1,
                          "timestamp": 1})
        .sort("timestamp", -1)
        .limit(limit)
    )
    return jsonify({"total": len(docs), "data": docs}), 200


# ------------------------------------------------------------------
# GET /stats
# Métricas agregadas: conteo por clase, accuracy global, avg probabilidad
# ------------------------------------------------------------------
@app.route("/stats", methods=["GET"])
def get_stats():
    pipeline = [
        {"$group": {
            "_id": "$prediccion",
            "total": {"$sum": 1},
            "correctas": {
                "$sum": {
                    "$cond": [
                        {"$eq": [
                            "$etiqueta_real",
                            {"$cond": [{"$eq": ["$prediccion", "riesgo"]}, 1, 0]}
                        ]}, 1, 0
                    ]
                }
            },
            "avg_probabilidad": {"$avg": "$probabilidad_riesgo"}
        }},
        {"$project": {
            "clase": "$_id",
            "total": 1,
            "correctas": 1,
            "avg_probabilidad": {"$round": ["$avg_probabilidad", 4]},
            "precision": {
                "$round": [
                    {"$cond": [
                        {"$eq": ["$total", 0]}, 0,
                        {"$divide": ["$correctas", "$total"]}
                    ]}, 4
                ]
            },
            "_id": 0
        }},
        {"$sort": {"clase": 1}}
    ]

    por_clase  = list(col.aggregate(pipeline))
    total_docs = col.count_documents({})
    total_riesgo = col.count_documents({"prediccion": "riesgo"})

    return jsonify({
        "total_pacientes_procesados": total_docs,
        "total_en_riesgo":           total_riesgo,
        "porcentaje_riesgo":         round(total_riesgo / total_docs * 100, 2) if total_docs else 0,
        "por_clase":                 por_clase
    }), 200


# ------------------------------------------------------------------
# GET /risk-summary
# Tabla de pacientes de alto riesgo (probabilidad_riesgo > umbral)
# Params: ?umbral=0.7 (default) ?limit=50
# ------------------------------------------------------------------
@app.route("/risk-summary", methods=["GET"])
def get_risk_summary():
    umbral = float(request.args.get("umbral", 0.7))
    limit  = int(request.args.get("limit", 50))

    docs = list(
        col.find(
            {"prediccion": "riesgo", "probabilidad_riesgo": {"$gte": umbral}},
            {"_id": 0}
        )
        .sort("probabilidad_riesgo", -1)
        .limit(limit)
    )

    return jsonify({
        "umbral_probabilidad": umbral,
        "total_alto_riesgo":   len(docs),
        "pacientes":           docs
    }), 200


# ------------------------------------------------------------------
# Health check
# ------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":    "ok",
        "timestamp": datetime.utcnow().isoformat()
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
