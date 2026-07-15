"""
================================================================================
SCRIPT: run_all_experiments.py
PROPOSITO: Orquestacion y ejecucion automatizada de todo el pipeline de experimentos.
TIPO DE ARCHIVO: Utilidad / Orquestador de Experimentos
================================================================================

DESCRIPCION:
Este script ejecuta secuencialmente todos los pasos del analisis batch offline:
1. Ejecuta el notebook de EDA (`eda_ieeecis.ipynb`) usando jupyter nbconvert para
   limpiar los datos y generar 'train_cleaned.csv' (opcional si ya existe).
2. Entrena el modelo base con mas de 300 variables (`training_model_IEEE_CIS.py`).
3. Entrena el modelo afinado (`training_model_IEEE_CIS_tuned.py`).
4. Entrena el modelo definitivo de 25 variables (`training_model_IEEE_CIS_tuned_top25.py`).
5. Ejecuta la extraccion de parametros para PaySim (`extract_paysim_params.py`).

Toda la salida de la consola se captura y se compila ordenadamente en el documento
`infraestructure_ieeecis/data_insight/resultados_ieee/Reporte_Resultados_Entrenamientos.md` para la sustentacion de la tesis.

ORDEN DE EJECUCION:
- Se puede ejecutar en cualquier momento para reproducir todos los experimentos 
  de forma automatizada con un solo comando:
  ```bash
  python run_all_experiments.py
  ```
================================================================================
"""

import os
import subprocess
import datetime
import sys

def run_command_and_capture(cmd_list, title):
    print(f"Ejecutando: {title} ...")
    report_content = f"## {title}\n\n"
    report_content += f"**Comando:** `{' '.join(cmd_list)}`\n\n"
    report_content += "```text\n"
    
    try:
        # Ejecutar y capturar salida
        result = subprocess.run(
            cmd_list, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True, 
            encoding='utf-8', 
            errors='replace'
        )
        report_content += result.stdout
        if result.returncode != 0:
            report_content += f"\n[ADVERTENCIA] El comando terminó con código de error: {result.returncode}\n"
    except Exception as e:
        report_content += f"Error al ejecutar: {str(e)}\n"
        
    report_content += "```\n\n---\n\n"
    print(f"Finalizado: {title}\n")
    return report_content

def main():
    # Rutas base dinamicas
    infra_dir = os.path.dirname(os.path.abspath(__file__))
    report_path = os.path.join(infra_dir, "data_insight", "resultados_ieee", "Reporte_Resultados_Entrenamientos.md")
    
    # Asegurar que el directorio de documentacion existe
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, "w", encoding='utf-8') as f:
        f.write("# Reporte Oficial de Ejecución: Modelos y Parámetros para PaySim\n")
        f.write(f"**Fecha de generación:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("Este documento contiene el registro íntegro de la ejecución del EDA, los distintos modelos de Isolation Forest y la extracción de parámetros. Los resultados aquí mostrados sustentan las decisiones tomadas para la segunda entrega del TFM.\n\n---\n\n")

    # Intentar usar el python del entorno Conda TFM para nbconvert para evitar conflictos de NumPy
    tfm_python = r"C:\Users\Jeaneth\anaconda3\envs\TFM\python.exe"
    if os.path.exists(tfm_python):
        jupyter_cmd = [tfm_python, "-m", "nbconvert"]
    else:
        jupyter_cmd = ["jupyter", "nbconvert"]

    steps = [
        {
            "title": "1. Análisis Exploratorio de Datos (EDA) y Limpieza",
            "cmd": jupyter_cmd + ["--execute", "--inplace", os.path.join(infra_dir, "notebooks", "eda_ieeecis.ipynb")]
        },
        {
            "title": "2. Modelo Base Isolation Forest (Todas las Variables)",
            "cmd": [sys.executable, os.path.join(infra_dir, "training", "training_model_IEEE_CIS.py")]
        },
        {
            "title": "3. Modelo Afinado (Hyperparameter Tuning - Todas las Variables)",
            "cmd": [sys.executable, os.path.join(infra_dir, "training", "training_model_IEEE_CIS_tuned.py")]
        },
        {
            "title": "4. Modelo Definitivo (Tuning - Top 25 Variables)",
            "cmd": [sys.executable, os.path.join(infra_dir, "training", "training_model_IEEE_CIS_tuned_top25.py")]
        },
        {
            "title": "5. Extracción de Parámetros Estadísticos (Entrada para PaySim)",
            "cmd": [sys.executable, os.path.join(infra_dir, "training", "extract_paysim_params.py")]
        }
    ]

    for step in steps:
        output = run_command_and_capture(step["cmd"], step["title"])
        with open(report_path, "a", encoding='utf-8') as f:
            f.write(output)

    print(f"Todo el proceso ha terminado. Reporte guardado en: {report_path}")

if __name__ == '__main__':
    main()
