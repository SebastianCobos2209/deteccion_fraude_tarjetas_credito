import os
import json
import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score,
    classification_report,
    confusion_matrix,
)
from xgboost import XGBClassifier
from confiig import (
    TRANSACTION_FILE, IDENTITY_FILE,
    MODEL_FILE, MODELS_DIR, FEATURES_FILE,
    NULL_THRESHOLD, TEST_SIZE,
    RANDOM_STATE, XGB_PARAMS,
)

# Carga de datos
def load_data():
    print("[1/6] Cargando CSVs...")
 
    transactions = pd.read_csv(TRANSACTION_FILE)
    identity     = pd.read_csv(IDENTITY_FILE)
 
    print(f"  Transacciones : {transactions.shape}")
    print(f"  Identidad     : {identity.shape}")
 
    # Merge por TransactionID (left join — no todas las tx tienen identidad)
    df = transactions.merge(identity, on="TransactionID", how="left")
    print(f"  Dataset unido : {df.shape}")
 
    return df
 
# Limpieza
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("[2/6] Limpiando datos...")
 
    # Eliminar columnas con demasiados nulos
    null_ratio = df.isnull().mean()
    cols_to_drop = null_ratio[null_ratio > NULL_THRESHOLD].index.tolist()
    df = df.drop(columns=cols_to_drop)
    print(f"  Columnas eliminadas por nulos (>{NULL_THRESHOLD*100}%): {len(cols_to_drop)}")
 
    # Separar target
    target = df["isFraud"]
    df = df.drop(columns=["isFraud", "TransactionID"])
 
    # Quedarse solo con columnas numéricas por ahora
    df = df.select_dtypes(include=[np.number])
    print(f"  Columnas numéricas restantes: {df.shape[1]}")
 
    # Imputar nulos restantes con la mediana
    df = df.fillna(df.median())
 
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
 
    print(f"  Features finales: {df.shape[1]}")
    return df
 
 
# Entrenar el modelo
def train_model(X_train, y_train):
    print("[4/6] Entrenando modelo XGBoost...")
 
    # Calcular peso para clases desbalanceadas (fraude es ~3.5% del dataset)
    scale = (y_train == 0).sum() / (y_train == 1).sum()
    print(f"  scale_pos_weight: {scale:.2f}  (clases desbalanceadas)")
 
    model = XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        scale_pos_weight=scale,   # compensa el desbalance
        use_label_encoder=False,
        eval_metric="auc",
        random_state=42,
        n_jobs=-1,
    )
 
    model.fit(X_train, y_train)
    return model

# Evaluar modelo
def evaluate_model(model, X_test, y_test):
    print("[5/6] Evaluando modelo...")
 
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred       = model.predict(X_test)
 
    auc = roc_auc_score(y_test, y_pred_proba)
    print(f"\n  AUC-ROC : {auc:.4f}")
    print(f"\n  Reporte de clasificación:\n{classification_report(y_test, y_pred)}")
    print(f"\n  Matriz de confusión:\n{confusion_matrix(y_test, y_pred)}")
 
    return auc
 
 
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
    print("\n✅ Entrenamiento completado.")
 
 
if __name__ == "__main__":
    main()