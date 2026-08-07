"""
================================================================================
SCRIPT: training_model_IEEE_CIS_tuned.py
PROPOSITO: Entrenamiento del Isolation Forest Afinado (Hyperparameter Tuning - Todas las Variables).
TIPO DE ARCHIVO: Entrenamiento / Modelado
================================================================================

DESCRIPCION:
Este script entrena un Isolation Forest con hiperparámetros afinados (n_estimators=500,
max_samples=2048, max_features=0.8) utilizando todas las variables del dataset limpio.
Establece las métricas óptimas que se pueden alcanzar cuando se usan las +300 variables,
sirviendo como benchmark de calidad/rendimiento frente a modelos más ligeros.

ORDEN DE EJECUCION (FASE DE ENTRENAMIENTO Y AFINACIÓN DE MODELOS):
1. [Notebook] `eda_ieeecis.ipynb` (Limpieza inicial y generación de train_cleaned.csv).
2. Ejecutar `training_model_IEEE_CIS.py` (Modelo base).
3. EJECUTAR ESTE SCRIPT (`training_model_IEEE_CIS_tuned.py`) para registrar el modelo afinado con todas las variables.
4. Ejecutar `find_top_features.py` (Ranking e importancia de variables).
5. Ejecutar `training_model_IEEE_CIS_tuned_top25.py` (Modelo optimizado y definitivo).
================================================================================
"""

import os
import json
import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score,
    f1_score,
    classification_report,
    confusion_matrix,
)
from sklearn.ensemble import IsolationForest
from confiig import (
    TRANSACTION_FILE, IDENTITY_FILE,
    MODEL_FILE, MODELS_DIR, FEATURES_FILE,
    NULL_THRESHOLD, TEST_SIZE,
    RANDOM_STATE, XGB_PARAMS,
)

# Carga de datos
def load_data():
    print("[1/6] Cargando dataset limpio...")
    
    # Obtenemos la ruta absoluta basada en la ubicación de este script
    # para evitar problemas dependiendo de desde dónde se ejecute
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cleaned_path = os.path.join(current_dir, '..', 'data_insight', 'data_ieee', 'train_cleaned.csv')
    
    if os.path.exists(cleaned_path):
        df = pd.read_csv(cleaned_path)
        print(f"  Dataset limpio cargado : {df.shape}")
    else:
        raise FileNotFoundError(f"No se encontró {cleaned_path}. Debes ejecutar el notebook EDA primero para generarlo.")
        
    return df
 
# Limpieza
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("[2/6] Separando target y features...")
 
    # Separar target
    target = df["isFraud"]
    
    # Eliminar identificadores y el target
    cols_to_drop = ["isFraud", "TransactionID"]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
 
    print(f"  Columnas restantes para modelado: {df.shape[1]}")
    return df, target
 
 

# Feature engineering
def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    print("[3/6] Feature engineering...")
 
    # Ratio del monto respecto al promedio (detecta transacciones anómalas)
    if "TransactionAmt" in df.columns:
        mean_amt = df["TransactionAmt"].mean()
        df["amt_ratio"] = df["TransactionAmt"] / (mean_amt + 1e-9)
 
    # Log del monto (suaviza outliers)
    if "TransactionAmt" in df.columns:
        df["log_amt"] = np.log1p(df["TransactionAmt"])
        
    # Procesar variables categóricas
    from sklearn.preprocessing import LabelEncoder
    cat_cols = df.select_dtypes(exclude=[np.number]).columns
    print(f"  Codificando {len(cat_cols)} variables categóricas...")
    
    # Para simplificar, usaremos LabelEncoding para todas las categóricas.
    # Isolation Forest puede procesar estas representaciones numéricas 
    # (Label Encoding es más eficiente en RAM que el One-Hot Encoding masivo).
    le = LabelEncoder()
    for col in cat_cols:
        df[col] = le.fit_transform(df[col].astype(str))
 
    print(f"  Features finales: {df.shape[1]}")
    return df
 
 
# Entrenar el modelo
def train_model(X_train, y_train):
    print("[4/6] Entrenando modelo Isolation Forest...")
 
    # Isolation Forest es un algoritmo de detección de anomalías.
    # El parámetro contamination indica la proporción esperada de outliers (fraudes).
    contamination = (y_train == 1).mean()
    if contamination <= 0 or contamination >= 0.5:
        contamination = 0.035  # valor por defecto si el cálculo falla
        
    print(f"  Contamination estimada (tasa de fraude): {contamination:.4f}")
 
    model = IsolationForest(
        n_estimators=500,
        max_samples=2048,
        max_features=0.8,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
 
    model.fit(X_train)
    return model

# Evaluar modelo
def evaluate_model(model, X_test, y_test):
    import time
    print("[5/6] Evaluando modelo Isolation Forest...")
 
    # Medir latencia de predicción
    start_time = time.time()
    y_pred_score = -model.decision_function(X_test)
    y_pred = model.predict(X_test)
    end_time = time.time()
    
    latency_ms = ((end_time - start_time) / len(X_test)) * 1000
    print(f"\n  Latencia promedio: {latency_ms:.4f} ms por transacción")
 
    # Mapear predicciones de IsolationForest (-1: anomalía/fraude, 1: normal)
    y_pred = np.where(y_pred == -1, 1, 0)
 
    auc = roc_auc_score(y_test, y_pred_score)
    f1 = f1_score(y_test, y_pred)
    
    print(f"  AUC-ROC : {auc:.4f}")
    print(f"  F1-Score: {f1:.4f}")
    print(f"\n  Reporte de clasificación:\n{classification_report(y_test, y_pred)}")
    print(f"\n  Matriz de confusión:\n{confusion_matrix(y_test, y_pred)}")
 
    return auc, f1
 
 
# Guardar el modelo y features
def save_artifacts(model, feature_names: list):
    print("[6/6] Guardando modelo y features...")
 
    os.makedirs(MODELS_DIR, exist_ok=True)
 
    # Guardar modelo
    joblib.dump(model, MODEL_FILE)
    print(f"  Modelo guardado en  : {MODEL_FILE}")
 
    # Guardar lista de columnas — Spark la usará para alinear features
    with open(FEATURES_FILE, "w") as f:
        json.dump(feature_names, f, indent=2)
    print(f"  Features guardadas en: {FEATURES_FILE}")
 
 
# MAIN
def main():
    print("=" * 50)
    print("  Entrenamiento IEEE-CIS Fraud Detection")
    print("=" * 50)
 
    # Cargar
    df = load_data()
 
    # Limpiar
    df, target = clean_data(df)
 
    # Feature engineering
    df = feature_engineering(df)
 
    # Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        df, target, test_size=0.2, random_state=42, stratify=target
    )
    print(f"\n  Train: {X_train.shape} | Test: {X_test.shape}")
 
    # Entrenar
    model = train_model(X_train, y_train)
 
    # Evaluar
    evaluate_model(model, X_test, y_test)

    # Guardar
    save_artifacts(model, list(df.columns))
    print("\nEntrenamiento completado.")
 
 
if __name__ == "__main__":
    main()