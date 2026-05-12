@echo off
REM ================================================================
REM topics.bat
REM Crear topics Kafka para el sistema Vertexon + PySpark
REM
REM ARQUITECTURA:
REM   Vertexon Mock
REM        ↓
REM   Producer Python
REM   ├──→ vertexon.usuarios          (perfiles de usuario)
REM   ├──→ vertexon.tarjetas          (datos de tarjeta, CVV encriptado)
REM   └──→ transactions.raw           (transacciones en bruto)
REM              ↓
REM          PySpark
REM   ├──→ transactions.enriched      (transaccion + usuario + tarjeta)
REM   ├──→ fraud.scores               (score ML por transaccion)
REM   └──→ fraud.alerts               (solo fraudes confirmados)
REM
REM RETENCIONES:
REM   7 dias  = 604800000 ms   (transacciones activas)
REM   30 dias = 2592000000 ms  (fraudes, auditoria)
REM   1 dia   = 86400000 ms    (scores, alta rotacion)
REM   90 dias = 7776000000 ms  (usuarios y tarjetas, cambian poco)
REM ================================================================

echo.
echo ================================================================
echo  Creando topics Kafka - Sistema Vertexon + PySpark
echo ================================================================
echo.

REM ────────────────────────────────────────────────────────────────
REM CAPA 1: ENTIDADES BASE (Producer Python → Kafka)
REM Retención larga porque usuarios y tarjetas cambian poco.
REM 1 partición porque el volumen es bajo y el orden importa
REM (no quieres procesar la misma tarjeta en paralelo desordenada).
REM ────────────────────────────────────────────────────────────────

docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic vertexon.usuarios ^
  --partitions 1 ^
  --replication-factor 1 ^
  --config retention.ms=7776000000 ^
  --config cleanup.policy=compact
echo [OK] vertexon.usuarios       (90 dias, compactado - guarda el ultimo estado por usuarioID)

docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic vertexon.tarjetas ^
  --partitions 1 ^
  --replication-factor 1 ^
  --config retention.ms=7776000000 ^
  --config cleanup.policy=compact
echo [OK] vertexon.tarjetas       (90 dias, compactado - guarda el ultimo estado por tarjetaID)

REM ────────────────────────────────────────────────────────────────
REM CAPA 2: TRANSACCIONES EN BRUTO (Producer Python → Kafka)
REM 3 particiones para paralelismo en PySpark.
REM max.message.bytes sube a 1MB por si la transaccion trae campos extra.
REM ────────────────────────────────────────────────────────────────

docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic transactions.raw ^
  --partitions 3 ^
  --replication-factor 1 ^
  --config retention.ms=604800000 ^
  --config max.message.bytes=1048576
echo [OK] transactions.raw        (7 dias, 3 particiones)

REM ────────────────────────────────────────────────────────────────
REM CAPA 3: PROCESAMIENTO PySpark → Kafka
REM transactions.enriched: transaccion + datos de usuario + tarjeta
REM fraud.scores:          score del modelo ML (0.0 a 1.0) por tx
REM fraud.alerts:          solo las transacciones marcadas esFraude=1
REM ────────────────────────────────────────────────────────────────

docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic transactions.enriched ^
  --partitions 3 ^
  --replication-factor 1 ^
  --config retention.ms=604800000
echo [OK] transactions.enriched   (7 dias, 3 particiones)

docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic fraud.scores ^
  --partitions 3 ^
  --replication-factor 1 ^
  --config retention.ms=86400000
echo [OK] fraud.scores            (1 dia - alta rotacion, un score por tx)

docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic fraud.alerts ^
  --partitions 1 ^
  --replication-factor 1 ^
  --config retention.ms=2592000000
echo [OK] fraud.alerts            (30 dias - auditoria y alertas criticas)

REM ────────────────────────────────────────────────────────────────
REM CAPA 4: DEAD LETTER QUEUE
REM Transacciones que fallaron al procesarse en PySpark.
REM Esencial para no perder datos en produccion.
REM ────────────────────────────────────────────────────────────────

docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic transactions.dlq ^
  --partitions 1 ^
  --replication-factor 1 ^
  --config retention.ms=2592000000
echo [OK] transactions.dlq        (30 dias - errores de procesamiento)

echo.
echo ================================================================
echo  Verificando todos los topics creados:
echo ================================================================
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo.
echo ================================================================
echo  RESUMEN DE FLUJO:
echo.
echo  Producer Python
echo    vertexon.usuarios    ^<-- perfiles de usuario (90 dias)
echo    vertexon.tarjetas    ^<-- tarjetas + CVV enc  (90 dias)
echo    transactions.raw     ^<-- transacciones brutas (7 dias)
echo           ^|
echo        PySpark
echo    transactions.enriched ^<-- tx + usuario + tarjeta
echo    fraud.scores          ^<-- score 0.0-1.0 por tx
echo    fraud.alerts          ^<-- solo esFraude=1
echo    transactions.dlq      ^<-- errores (Dead Letter Queue)
echo ================================================================
echo.
pause