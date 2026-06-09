"""
config/settings.py
Configuración central del pipeline.
Sincronizado con docker-compose.yml (apache/spark:3.5.1)

Variables de entorno configurables en docker-compose → spark-submit:
  KAFKA_BROKER, MONGO_URI, SPARK_MASTER,
  WARMUP_SIZE, FRAUD_THRESHOLD, RETRAIN_INTERVAL, MONGO_ENABLED
"""
import os

# ── Kafka ─────────────────────────────────────────────────────
# Interno entre contenedores: kafka:29092
# Externo desde tu máquina: localhost:9092
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_RAW         = "transactions.raw"
TOPIC_ALERTS      = "fraud.alerts"

# ── Spark ─────────────────────────────────────────────────────
# Imagen: apache/spark:3.5.1
# Master port: 7077  |  Web UI: 4040
SPARK_MASTER      = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_APP_NAME    = "FraudeDeteccionTarjetas"
CHECKPOINT_PATH   = "/tmp/spark_checkpoints/fraud_pipeline"
MAX_OFFSETS_BATCH = 200          # máx transacciones por micro-batch
TRIGGER_INTERVAL  = "5 seconds"  # frecuencia del micro-batch

# ── MongoDB ───────────────────────────────────────────────────
# Credenciales: admin / tfm2026
# Base de datos: fraude_db
# MONGO_ENABLED=false → desactivado (sin MongoDB aún)
# MONGO_ENABLED=true  → activar cuando MongoDB esté listo
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:tfm2026@mongodb:27017/fraude_db?authSource=admin"
)
MONGO_DB      = "fraude_db"
COL_ENRICHED  = "transactions_enriched"
COL_ALERTS    = "fraud_alerts"
COL_METRICS   = "model_metrics"
MONGO_ENABLED = os.getenv("MONGO_ENABLED", "false").lower() == "true"

# ── Modelo Isolation Forest ───────────────────────────────────
WARMUP_SIZE      = int(os.getenv("WARMUP_SIZE",      "500"))
FRAUD_THRESHOLD  = float(os.getenv("FRAUD_THRESHOLD", "0.5"))
RETRAIN_INTERVAL = int(os.getenv("RETRAIN_INTERVAL", "300"))
CONTAMINATION    = 0.035   # tasa de fraude real del dataset IEEE-CIS
MODEL_PATH       = "/opt/spark_models/isolation_forest.joblib"
SCALER_PATH      = "/opt/spark_models/scaler.joblib"

# ── Features IEEE-CIS (19 variables numéricas) ────────────────
FEATURE_COLS = [
    "TransactionAmt", "TransactionDT", "card1", "addr1", "addr2",
    "C1", "C13", "D1", "V314",
    "V201", "V243", "V257", "C7", "V242",
    "V45", "V246", "V200", "V258", "C14",
]
AMT_MEAN = 135.027   # media de TransactionAmt en IEEE-CIS
AMT_STD  = 239.163   # std  de TransactionAmt en IEEE-CIS