"""
================================================================================
SCRIPT: training_model_IEEE_CIS_tuned_top25.py
PROPOSITO: Entrenamiento del Modelo Definitivo de Isolation Forest (Top 25 Variables).
TIPO DE ARCHIVO: Entrenamiento / Modelado (Definitivo)
================================================================================

DESCRIPCION:
Este script entrena el modelo de producción final (Isolation Forest) utilizando las 
25 variables definitivas seleccionadas (Top 15 + 10 variables de red y comportamiento
clave obtenidas con RandomForest). Este modelo balancea perfectamente el F1-Score 
(sube a ~0.29, superando el benchmark de 360 variables) con la latencia en streaming
(mantenida en ~0.15ms). El artefacto generado se exporta como `model_top25.pkl` para 
ser consumido en caliente por Spark Streaming.

ORDEN DE EJECUCION (FASE DE ENTRENAMIENTO Y AFINACIÓN DE MODELOS):
1. [Notebook] `eda_ieeecis.ipynb` (Limpieza inicial).
2. Ejecutar `training_model_IEEE_CIS.py` (Modelo base).
3. Ejecutar `training_model_IEEE_CIS_tuned.py` (Modelo afinado).
4. Ejecutar `find_top_features.py` (Identificar variables relevantes).
5. EJECUTAR ESTE SCRIPT (`training_model_IEEE_CIS_tuned_top25.py`) para entrenar el modelo final.
6. Ejecutar `extract_paysim_params.py` (Extraer estadísticos para simulador).
================================================================================
"""

import os
import json
import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score,
    f1_score,
    classification_report,
    confusion_matrix,
)
from confiig import (
    MODEL_FILE, MODELS_DIR, FEATURES_FILE
)

# Definimos las variables pactadas para el simulador (extendido a 25)
TOP_25_FEATURES = [
    "TransactionAmt", "TransactionDT", "ProductCD", 
    "card1", "card4", "card6", "P_emaildomain", 
    "addr1", "addr2", "DeviceType", "DeviceInfo", 
    "C1", "C13", "D1", "V314",
    # 10 variables extra identificadas por importancia (RF)
    "V201", "V243", "V257", "C7", "V242", 
    "V45", "V246", "V200", "V258", "C14"
]

# Carga de datos
def load_data():
    print("[1/6] Cargando dataset limpio...")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cleaned_path = os.path.join(current_dir, '..', 'data_insight', 'data_ieee', 'train_cleaned.csv')
    
    if os.path.exists(cleaned_path):
        df = pd.read_csv(cleaned_path)
        print(f"  Dataset limpio cargado : {df.shape}")
    else:
        raise FileNotFoundError(f"No se encontró {cleaned_path}. Ejecuta el notebook EDA primero.")
        
    return df
 
# Limpieza y Filtrado de TOP 25
def clean_data(df: pd.DataFrame):
    print("[2/6] Filtrando únicamente las Top 25 variables predictivas...")
 
    target = df["isFraud"]
    
    # Filtrar solo las 25 columnas seleccionadas
    features_to_keep = [col for col in TOP_25_FEATURES if col in df.columns]
    df = df[features_to_keep].copy()
 
    print(f"  Variables seleccionadas ({len(df.columns)}): {df.columns.tolist()}")
    return df, target
 
# Feature engineering
def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    print("[3/6] Feature engineering (Agregando ratios)...")
 
    # Ratio del monto respecto al promedio
    if "TransactionAmt" in df.columns:
        mean_amt = df["TransactionAmt"].mean()
        df["amt_ratio"] = df["TransactionAmt"] / (mean_amt + 1e-9)
        df["log_amt"] = np.log1p(df["TransactionAmt"])
        
    # Procesar variables categóricas
    from sklearn.preprocessing import LabelEncoder
    cat_cols = df.select_dtypes(exclude=[np.number]).columns
    
    le = LabelEncoder()
    for col in cat_cols:
        df[col] = le.fit_transform(df[col].astype(str))
 
    print(f"  Features finales para entrenar: {df.shape[1]}")
    return df
 
# Entrenar el modelo
def train_model(X_train, y_train):
    print("[4/6] Entrenando Isolation Forest con Top 25 variables...")
 
    contamination = (y_train == 1).mean()
    if contamination <= 0 or contamination >= 0.5:
        contamination = 0.035
        
    model = IsolationForest(
        n_estimators=500,
        max_samples=2048,
        max_features=1.0, # Usar todas las características (ya las filtramos a 15)
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
 
    model.fit(X_train)
    return model

# Evaluar modelo
def evaluate_model(model, X_test, y_test):
    import time
    print("[5/6] Evaluando modelo...")
 
    start_time = time.time()
    y_pred_score = -model.decision_function(X_test)
    y_pred = model.predict(X_test)
    end_time = time.time()
    
    latency_ms = ((end_time - start_time) / len(X_test)) * 1000
    print(f"\n  Latencia promedio: {latency_ms:.4f} ms por transacción")
 
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
    print("[6/6] Guardando modelo y listado estricto de variables...")
 
    os.makedirs(MODELS_DIR, exist_ok=True)
 
    joblib.dump(model, MODEL_FILE.replace(".pkl", "_top25.pkl"))
    print(f"  Modelo guardado como: {MODEL_FILE.replace('.pkl', '_top25.pkl')}")
 
    # Guardar las variables que el simulador NECESITA saber que existen
    features_path = FEATURES_FILE.replace(".json", "_top25.json")
    with open(features_path, "w") as f:
        json.dump(feature_names, f, indent=2)
    print(f"  Features estáticas guardadas en: {features_path}")
 
# MAIN
def main():
    print("=" * 60)
    print(" Entrenamiento IEEE-CIS Fraud Detection - MODO TOP 25 VARIABLES")
    print("=" * 60)
 
    df = load_data()
    df, target = clean_data(df)
    df = feature_engineering(df)
 
    X_train, X_test, y_train, y_test = train_test_split(
        df, target, test_size=0.2, random_state=42, stratify=target
    )
 
    model = train_model(X_train, y_train)
    evaluate_model(model, X_test, y_test)
    save_artifacts(model, list(df.columns))
    print("\nEntrenamiento Top 25 completado. Entregable listo para debatir.")
 
if __name__ == "__main__":
    main()
