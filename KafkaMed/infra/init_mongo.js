// infra/init_mongo.js
db = db.getSiblingDB("kafkamed");

db.createCollection("predicciones");

db.predicciones.createIndex({ prediccion: 1 });
db.predicciones.createIndex({ probabilidad_riesgo: -1 });
db.predicciones.createIndex({ timestamp: -1 });
db.predicciones.createIndex({ "Age": 1 });

print("✅ Base de datos 'kafkamed' e índices creados.");
