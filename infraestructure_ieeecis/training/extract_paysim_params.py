"""
================================================================================
SCRIPT: extract_paysim_params.py
PROPOSITO: Extraccion de parametros estadisticos de las Top 25 variables para PaySim.
TIPO DE ARCHIVO: Analisis de Datos / Pipeline de Simulacion
================================================================================

DESCRIPCION:
Este script calcula y extrae metricas estadisticas clave (medias, varianzas, modas
y frecuencias de categorias) y la matriz de correlacion de las 25 variables predictivas
seleccionadas del dataset limpio de IEEE. Exporta los resultados a
`paysim_statistical_params.json` y `paysim_correlation_matrix.csv`.
Estos archivos son cruciales para parametrizar el simulador de transacciones PaySim,
permitiendo que genere eventos sinteticos con la misma distribucion de probabilidad
que el dataset original.

ORDEN DE EJECUCION (FASE DE ENTRENAMIENTO Y CONFIGURACIÓN DE SIMULACIÓN):
1. [Notebook] `eda_ieeecis.ipynb` (Limpieza inicial).
2. Ejecutar entrenamientos de Isolation Forest.
3. Ejecutar `training_model_IEEE_CIS_tuned_top25.py` para entrenar el modelo definitivo.
4. EJECUTAR ESTE SCRIPT (`extract_paysim_params.py`) para generar las variables del simulador.
================================================================================
"""

import os
import json
import pandas as pd
import numpy as np

def extract_parameters():
    print("="*50)
    print("Extracción de Parámetros Estadísticos para PaySim")
    print("="*50)

    # 1. Definir rutas
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, '..', 'data_insight', 'data_ieee', 'train_cleaned.csv')
    output_dir = os.path.join(current_dir, '..', 'data_insight', 'paysim_params')
    
    os.makedirs(output_dir, exist_ok=True)
    
    if not os.path.exists(data_path):
        print(f"[Error] No se encontró el dataset limpio en: {data_path}")
        print("Asegúrate de haber ejecutado el EDA primero para generar 'train_cleaned.csv'")
        return

    print("[1/4] Cargando dataset limpio...")
    df = pd.read_csv(data_path)
    
    # 2. Extraer Tasa de Contaminación (Desbalance de Clase)
    print("[2/4] Calculando desbalance de clase...")
    if 'isFraud' in df.columns:
        fraud_counts = df['isFraud'].value_counts(normalize=True).to_dict()
        contamination_rate = fraud_counts.get(1, 0.0)
    else:
        print("[Advertencia] Columna 'isFraud' no encontrada. Se asume 3.5% por defecto.")
        contamination_rate = 0.035

    # 3. Definir las 25 variables predictivas explícitas
    print("[3/4] Filtrando las 25 variables requeridas...")
    top_25_features = [
        'TransactionAmt', 'TransactionDT', 'ProductCD',
        'card1', 'card4', 'card6', 'P_emaildomain', 'addr1', 'addr2',
        'DeviceType', 'DeviceInfo', 'C1', 'C13', 'D1', 'V314',
        'V201', 'V243', 'V257', 'C7', 'V242',
        'V45', 'V246', 'V200', 'V258', 'C14'
    ]
    
    # Validar que existan
    feature_cols = [c for c in top_25_features if c in df.columns]
    
    stats_dict = {
        "global_parameters": {
            "contamination_rate": float(contamination_rate),
            "total_transactions_analyzed": int(len(df))
        },
        "features": {}
    }

    for col in feature_cols:
        # Si es numérica
        if pd.api.types.is_numeric_dtype(df[col]):
            stats_dict["features"][col] = {
                "type": "numeric",
                "mean": float(df[col].mean()),
                "median": float(df[col].median()),
                "std": float(df[col].std()),
                "variance": float(df[col].var()),
                "min": float(df[col].min()),
                "max": float(df[col].max())
            }
        # Si es categórica / object
        else:
            # Tomar el top 5 de valores más frecuentes (Modas)
            top_values = df[col].value_counts(normalize=True).head(5).to_dict()
            stats_dict["features"][col] = {
                "type": "categorical",
                "top_frequencies": {str(k): float(v) for k, v in top_values.items()}
            }

    # Guardar en JSON
    json_path = os.path.join(output_dir, 'paysim_statistical_params.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(stats_dict, f, indent=4)
    print(f"  -> Parámetros guardados en: {json_path}")

    # 4. Extraer y Guardar Matriz de Correlación (Solo numéricas)
    print("[4/4] Calculando matriz de correlación...")
    numeric_df = df[feature_cols].select_dtypes(include=[np.number])
    corr_matrix = numeric_df.corr()
    
    corr_path = os.path.join(output_dir, 'paysim_correlation_matrix.csv')
    corr_matrix.to_csv(corr_path)
    print(f"  -> Matriz de correlación guardada en: {corr_path}")

    print("\n¡Extracción completada con éxito!")
    print("Tu compañero puede utilizar los archivos en la carpeta 'infraestructure_ieeecis/data_insight/paysim_params' para configurar PaySim.")

if __name__ == "__main__":
    extract_parameters()
