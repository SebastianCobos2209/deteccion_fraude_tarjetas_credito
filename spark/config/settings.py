"""
config/settings.py
Configuración central del pipeline.
Sincronizado con docker-compose.yml (apache/spark:3.5.1)
"""
import os

# ── Kafka ─────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_RAW      = "transactions.raw"
TOPIC_ALERTS   = "fraud.alerts"
TOPIC_USUARIOS = "vertexon.usuarios"
TOPIC_TARJETAS = "vertexon.tarjetas" 

# ── Spark ─────────────────────────────────────────────────────
SPARK_MASTER      = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_APP_NAME    = "FraudeDeteccionTarjetas"
CHECKPOINT_PATH   = "/tmp/spark_checkpoints/fraud_pipeline"
CHECKPOINT_USERS  = "/tmp/spark_checkpoints/usuarios_pipeline" 
CHECKPOINT_CARDS  = "/tmp/spark_checkpoints/tarjetas_pipeline" 
MAX_OFFSETS_BATCH = 200
TRIGGER_INTERVAL  = "5 seconds"

# ── MongoDB ───────────────────────────────────────────────────
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:tfm2026@mongodb:27017/fraude_db?authSource=admin"
)
MONGO_DB      = "fraude_db"
COL_RAW       = "transactions_raw"        
COL_ENRICHED  = "transactions_enriched"
COL_ALERTS    = "fraud_alerts"
COL_METRICS   = "model_metrics"
COL_USERS     = "user_profiles"           
COL_CARDS     = "cards"                  
MONGO_ENABLED = os.getenv("MONGO_ENABLED", "false").lower() == "true"

# ── Modelo Isolation Forest ───────────────────────────────────
WARMUP_SIZE      = int(os.getenv("WARMUP_SIZE",      "500"))
FRAUD_THRESHOLD  = float(os.getenv("FRAUD_THRESHOLD", "0.5"))
RETRAIN_INTERVAL = int(os.getenv("RETRAIN_INTERVAL", "300"))
CONTAMINATION    = 0.035
MODEL_PATH       = "/opt/spark_models/isolation_forest.joblib"
SCALER_PATH      = "/opt/spark_models/scaler.joblib"

# ── Features IEEE-CIS (19 variables numéricas) ────────────────
FEATURE_COLS = [
    "TransactionAmt", "TransactionDT", "card1", "addr1", "addr2",
    "C1", "C13", "D1", "V314",
    "V201", "V243", "V257", "C7", "V242",
    "V45", "V246", "V200", "V258", "C14",
]
AMT_MEAN = 135.027
AMT_STD  = 239.163