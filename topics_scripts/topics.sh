#!/bin/bash
# ================================================================
# topics.sh
# Ejecutar DESPUÉS de que docker-compose esté levantado
# Uso: bash topics.sh
# ================================================================

export PATH=$PATH:/mnt/c/Program\ Files/Docker/Docker/resources/bin


KAFKA_CONTAINER="kafka"
BOOTSTRAP="localhost:9092"


echo "Esperando que Kafka esté listo..."
sleep 5

echo ""
echo "Creando topics Kafka..."
echo "──────────────────────────────────────────"

# ── TOPIC 1: transactions.raw ─────────────────
# Recibe TODAS las transacciones en bruto tal como llegan
# 3 particiones para procesar en paralelo
# retención de 7 días (168h configurado en docker-compose)
docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
  --bootstrap-server $BOOTSTRAP \
  --topic transactions.raw \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config max.message.bytes=1048576
echo "transactions.raw creado"

# ── TOPIC 2: transactions.enriched ───────────────
# Recibe transacciones ya enriquecidas por PySpark
# (con features calculadas: velocidad, delta_monto, etc.)
docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
  --bootstrap-server $BOOTSTRAP \
  --topic transactions.enriched \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000
echo "transactions.enriched creado"

# ── TOPIC 3: fraud.alerts ────────────────────────
# Recibe SOLO las transacciones que el modelo marcó como fraude
# Aquí se conectan los sistemas de alerta (email, bloqueo API, dashboard)
docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
  --bootstrap-server $BOOTSTRAP \
  --topic fraud.alerts \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=2592000000
echo "fraud.alerts creado"

# ── TOPIC 4: fraud.scores ────────────────────────
# Publica el score de fraude de cada transacción (0.0 a 1.0)
docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
  --bootstrap-server $BOOTSTRAP \
  --topic fraud.scores \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000
echo "fraud.scores creado"

echo ""
echo "Verificando topics creados:"
echo "──────────────────────────────────────────"
docker exec $KAFKA_CONTAINER \
  kafka-topics --list \
  --bootstrap-server $BOOTSTRAP

echo "Todos los topics fueron creados correctamente."
#Para ver detalles de un topic:
# docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092 --topic transactions.raw
#Para enviar un mensaje de prueba:
#docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic transactions.raw
#Para consumir mensajes:
#docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions.raw --from-beginning