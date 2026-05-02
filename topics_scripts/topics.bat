@echo off
REM ================================================================
REM topics.bat
REM Crear topics Kafka desde Windows
REM Doble clic o ejecutar en CMD / PowerShell
REM ================================================================

echo Creando topics Kafka...
echo ──────────────────────────────────────────

REM TOPIC 1: transactions.raw - todas las transacciones en bruto
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic transactions.raw --partitions 3 --replication-factor 1 --config retention.ms=604800000 --config max.message.bytes=1048576
echo transactions.raw creado

REM TOPIC 2: transactions.enriched - enriquecidas por PySpark
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic transactions.enriched --partitions 3 --replication-factor 1 --config retention.ms=604800000
echo transactions.enriched creado

REM TOPIC 3: fraud.alerts - solo fraudes detectados, retencion 30 dias
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic fraud.alerts --partitions 1 --replication-factor 1 --config retention.ms=2592000000
echo fraud.alerts creado

REM TOPIC 4: fraud.scores - score de cada transaccion, retencion 1 dia
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic fraud.scores --partitions 3 --replication-factor 1 --config retention.ms=86400000
echo fraud.scores creado

echo.
echo Verificando topics creados:
echo ──────────────────────────────────────────
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo.
echo Todos los topics fueron creados correctamente.
pause