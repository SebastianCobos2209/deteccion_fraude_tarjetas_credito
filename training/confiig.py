"""
Configuración centralizada (Training)
rutas y parámetros.
"""
import os

# RUTAS BASE
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR   = os.path.join(BASE_DIR, "data", "ieee")
MODELS_DIR = os.path.join(BASE_DIR, "models")

# ARCHIVOS DE ENTRADA
TRANSACTION_FILE = os.path.join(DATA_DIR, "train_transaction.csv")
IDENTITY_FILE    = os.path.join(DATA_DIR, "train_identity.csv")

# ARCHIVOS DE SALIDA
MODEL_FILE    = os.path.join(MODELS_DIR, "model.pkl")
FEATURES_FILE = os.path.join(MODELS_DIR, "features.json")

# PARÁMETROS DE LIMPIEZA
NULL_THRESHOLD = 0.5       # eliminar columnas con más del 50% de nulos

# PARÁMETROS DEL MODELO
TEST_SIZE    = 0.2         # 20% para evaluación
RANDOM_STATE = 42
XGB_PARAMS = {
    "n_estimators"  : 300,
    "max_depth"     : 6,
    "learning_rate" : 0.05,
    "eval_metric"   : "auc",
    "random_state"  : RANDOM_STATE,
    "n_jobs"        : -1,
}