"""
================================================================================
SCRIPT: confiig.py
PROPOSITO: Configuracion centralizada de rutas y parametros para los modelos de entrenamiento.
TIPO DE ARCHIVO: Modulo de Configuracion / Utilidad
================================================================================

DESCRIPCION:
Este archivo centraliza las rutas absolutas para los datasets de IEEE, las carpetas
donde se guardan los modelos y los parametros generales utilizados durante la fase de
entrenamiento. Todos los scripts de entrenamiento importan este modulo para mantener
consistencia en las rutas de entrada/salida.
================================================================================
"""

import os

# RUTAS BASE
INFRA_DIR  = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR   = os.path.join(INFRA_DIR, "data_insight", "data_ieee")
MODELS_DIR = os.path.abspath(os.path.join(INFRA_DIR, "..", "models"))

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