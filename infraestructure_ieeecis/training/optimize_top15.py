"""
================================================================================
SCRIPT: optimize_top15.py
PROPOSITO: Grid Search para optimizar hiperparametros sobre el set de Top 15 variables.
TIPO DE ARCHIVO: Experimentacion / Optimizacion de Hiperparametros
================================================================================

DESCRIPCION:
Este script realiza un Grid Search (n_estimators, max_samples, contamination)
sobre el conjunto inicial acotado de 15 variables predictivas. Permite identificar
si ajustar hiperparametros especificos de Isolation Forest ayuda a mitigar la
perdida de F1-Score al reducir drasticamente las dimensiones del dataset de entrada.

ORDEN DE EJECUCION (FASE DE EXPERIMENTACIÓN Y ANALISIS):
- Este script se ejecuta de forma complementaria o paralela durante la fase de
  optimizacion de variables, para evaluar la sensibilidad del algoritmo de anomalías.
================================================================================
"""

import os
import json
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, f1_score
from sklearn.preprocessing import LabelEncoder

TOP_15_FEATURES = [
    "TransactionAmt", "TransactionDT", "ProductCD", 
    "card1", "card4", "card6", "P_emaildomain", 
    "addr1", "addr2", "DeviceType", "DeviceInfo", 
    "C1", "C13", "D1", "V314"
]

def main():
    print("Cargando datos...")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cleaned_path = os.path.join(current_dir, '..', '..', 'data', 'ieee', 'train_cleaned.csv')
    df = pd.read_csv(cleaned_path)
    
    target = df["isFraud"]
    features_to_keep = [col for col in TOP_15_FEATURES if col in df.columns]
    df = df[features_to_keep].copy()
    
    if "TransactionAmt" in df.columns:
        mean_amt = df["TransactionAmt"].mean()
        df["amt_ratio"] = df["TransactionAmt"] / (mean_amt + 1e-9)
        df["log_amt"] = np.log1p(df["TransactionAmt"])
        
    cat_cols = df.select_dtypes(exclude=[np.number]).columns
    le = LabelEncoder()
    for col in cat_cols:
        df[col] = le.fit_transform(df[col].astype(str))
        
    X_train, X_test, y_train, y_test = train_test_split(
        df, target, test_size=0.2, random_state=42, stratify=target
    )
    
    # Grid Search Parameters
    n_estimators_list = [100, 300]
    max_samples_list = ['auto', 1024, 2048]
    contamination_list = [0.035, 0.05, 0.08]
    
    best_f1 = 0
    best_params = {}
    
    print("Iniciando Grid Search...")
    for n in n_estimators_list:
        for ms in max_samples_list:
            for c in contamination_list:
                print(f"Probando: n_estimators={n}, max_samples={ms}, contamination={c}")
                model = IsolationForest(
                    n_estimators=n,
                    max_samples=ms,
                    max_features=1.0,
                    contamination=c,
                    random_state=42,
                    n_jobs=-1
                )
                model.fit(X_train)
                y_pred = model.predict(X_test)
                y_pred = np.where(y_pred == -1, 1, 0)
                f1 = f1_score(y_test, y_pred)
                print(f"  -> F1-Score: {f1:.4f}")
                
                if f1 > best_f1:
                    best_f1 = f1
                    best_params = {'n_estimators': n, 'max_samples': ms, 'contamination': c}
                    
    print("\n===============================")
    print(f"MEJOR F1-SCORE: {best_f1:.4f}")
    print(f"MEJORES PARAMETROS: {best_params}")
    print("===============================")

if __name__ == "__main__":
    main()
