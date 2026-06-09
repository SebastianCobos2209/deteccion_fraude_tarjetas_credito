"""
================================================================================
SCRIPT: find_top_features.py
PROPOSITO: Identificacion y ranking de la relevancia de variables (Random Forest).
TIPO DE ARCHIVO: Analisis / Seleccion de Variables
================================================================================

DESCRIPCION:
Este script entrena un clasificador RandomForest rapido y ligero sobre el
dataset completo con el fin de calcular la importancia (relevancia estadistica)
de cada una de las variables predictoras respecto a la variable objetivo (isFraud).
Esto permite seleccionar el subconjunto optimo de variables adicionales (como
las familias V y C) para el modelo definitivo, logrando reducir el ruido y la latencia.

ORDEN DE EJECUCION (FASE DE ENTRENAMIENTO Y AFINACIÓN DE MODELOS):
1. [Notebook] `eda_ieeecis.ipynb` (Limpieza inicial).
2. Ejecutar `training_model_IEEE_CIS.py` (Modelo base).
3. Ejecutar `training_model_IEEE_CIS_tuned.py` (Modelo afinado).
4. EJECUTAR ESTE SCRIPT (`find_top_features.py`) para identificar las mejores variables.
5. Ejecutar `training_model_IEEE_CIS_tuned_top25.py` (Modelo optimizado y definitivo).
================================================================================
"""

import os
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

def find_top_features():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cleaned_path = os.path.join(current_dir, '..', '..', 'data', 'ieee', 'train_cleaned.csv')
    
    print(f"Cargando {cleaned_path}...")
    df = pd.read_csv(cleaned_path)
    
    # Manejar nulos
    df = df.fillna(-999)
    
    target = df['isFraud']
    X = df.drop(columns=['isFraud', 'TransactionID'], errors='ignore') 
    
    # Label encoding para categóricas
    cat_cols = X.select_dtypes(exclude=[np.number]).columns
    le = LabelEncoder()
    for col in cat_cols:
        X[col] = le.fit_transform(X[col].astype(str))
        
    print("Entrenando RandomForest para extraer importancia de variables...")
    # Usamos RandomForest rápido
    rf = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1)
    rf.fit(X, target)
    
    importances = rf.feature_importances_
    
    # Crear dataframe con las importancias
    feat_imp = pd.DataFrame({
        'Feature': X.columns,
        'Importance': importances
    }).sort_values(by='Importance', ascending=False)
    
    print("\n--- TOP 30 VARIABLES MÁS IMPORTANTES ---")
    print(feat_imp.head(30).to_string(index=False))

if __name__ == '__main__':
    find_top_features()
