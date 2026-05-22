# KafkaMed – Plataforma de Monitoreo Cardíaco en Streaming

KafkaMed es una plataforma Big Data desarrollada para el procesamiento de registros médicos en tiempo real utilizando Apache Kafka, Spark Structured Streaming, MongoDB, Flask, Docker y Power BI.

El sistema simula el ingreso continuo de pacientes, realiza predicciones de riesgo cardíaco mediante Machine Learning y visualiza los resultados dinámicamente.

---

# Tecnologías utilizadas

* Apache Kafka
* Apache Spark Structured Streaming
* MongoDB
* Flask API
* Docker & Docker Compose
* Jenkins CI/CD
* Power BI
* Scikit-Learn

---

# Arquitectura del sistema

El flujo general del sistema es:

Producer Kafka → Apache Kafka → Spark Streaming → MongoDB → Flask API → Power BI

---

# Estructura del proyecto

```text
KafkaMed/
│
├── api/
│   └── app.py
│
├── consumer/
│   └── consumer.py
│
├── producer/
│   └── producer.py
│
├── datos/
│   ├── heart.csv
│   └── modelo_cardiaco/
│
├── infra/
│   ├── init_mongo.js
│   └── Jenkinsfile
│
├── docker-compose.yml
│
└── README.md
```

---

# Dataset

El proyecto utiliza el dataset `heart.csv`, que contiene registros médicos de pacientes para predicción de enfermedades cardíacas.

Variables utilizadas:

* Age
* Sex
* ChestPainType
* RestingBP
* Cholesterol
* FastingBS
* RestingECG
* MaxHR
* ExerciseAngina
* Oldpeak
* ST_Slope

---

# Modelo de Machine Learning

Se entrenó un modelo Random Forest utilizando Scikit-Learn.

## Métricas obtenidas

| Métrica  | Resultado |
| -------- | --------- |
| Accuracy | 0.8591    |
| F1-Score | 0.8569    |
| AUC-ROC  | 0.9242    |

---

# Despliegue del sistema

## 1. Clonar repositorio

```bash
git clone https://github.com/Penia750/KafkaMed_repo.git
```

---

## 2. Entrar al proyecto

```bash
cd KafkaMed
```

---

## 3. Levantar contenedores Docker

```bash
docker compose up -d
```

---

## 4. Verificar contenedores

```bash
docker ps
```

---

# API REST

## Endpoint estadísticas

```text
http://localhost:5000/stats
```

## Endpoint resumen de riesgo

```text
http://localhost:5000/risk-summary
```

---

# Jenkins CI/CD

El proyecto incluye integración CI/CD utilizando Jenkins.

El pipeline automatiza:

* Clonado del repositorio
* Construcción de imágenes Docker
* Levantamiento de servicios
* Verificación de contenedores
* Validación de la API

---

# Power BI

Power BI consume los endpoints REST generados por Flask para construir dashboards dinámicos en tiempo real.

Visualizaciones implementadas:

* Distribución de riesgo
* Evolución temporal
* Correlación colesterol-riesgo
* Tabla de alertas activas

---

# Autor

Juan David Peña
