# Reporte Oficial de Ejecución: Modelos y Parámetros para PaySim
**Fecha de generación:** 2026-07-14 20:10:46

Este documento contiene el registro íntegro de la ejecución del EDA, los distintos modelos de Isolation Forest y la extracción de parámetros. Los resultados aquí mostrados sustentan las decisiones tomadas para la segunda entrega del TFM.

---

## 1. Análisis Exploratorio de Datos (EDA) y Limpieza

**Comando:** `C:\Users\Jeaneth\anaconda3\envs\TFM\python.exe -m nbconvert --execute --inplace D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\notebooks\eda_ieeecis.ipynb`

```text
[NbConvertApp] Converting notebook D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\notebooks\eda_ieeecis.ipynb to notebook
C:\Users\Jeaneth\anaconda3\envs\TFM\Lib\site-packages\zmq\_future.py:718: RuntimeWarning: Proactor event loop does not implement add_reader family of methods required for zmq. Registering an additional selector thread for add_reader support via tornado. Use `asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())` to avoid this warning.
  self._get_loop()
[NbConvertApp] Writing 557319 bytes to D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\notebooks\eda_ieeecis.ipynb
```

---

## 2. Modelo Base Isolation Forest (Todas las Variables)

**Comando:** `D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\.venv\Scripts\python.exe D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\training_model_IEEE_CIS.py`

```text
D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\training_model_IEEE_CIS.py:85: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
  df["amt_ratio"] = df["TransactionAmt"] / (mean_amt + 1e-9)
D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\training_model_IEEE_CIS.py:89: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
  df["log_amt"] = np.log1p(df["TransactionAmt"])
==================================================
  Entrenamiento IEEE-CIS Fraud Detection
==================================================
[1/6] Cargando dataset limpio...
  Dataset limpio cargado : (590540, 360)
[2/6] Separando target y features...
  Columnas restantes para modelado: 358
[3/6] Feature engineering...
  Codificando 26 variables categ�ricas...
  Features finales: 360

  Train: (472432, 360) | Test: (118108, 360)
[4/6] Entrenando modelo Isolation Forest...
  Contamination estimada (tasa de fraude): 0.0350
[5/6] Evaluando modelo Isolation Forest...

  Latencia promedio: 0.0579 ms por transacci�n
  AUC-ROC : 0.7780
  F1-Score: 0.2550

  Reporte de clasificaci�n:
              precision    recall  f1-score   support

           0       0.97      0.97      0.97    113975
           1       0.26      0.25      0.26      4133

    accuracy                           0.95    118108
   macro avg       0.61      0.61      0.61    118108
weighted avg       0.95      0.95      0.95    118108


  Matriz de confusi�n:
[[110910   3065]
 [  3081   1052]]
[6/6] Guardando modelo y features...
  Modelo guardado en  : D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\models\model.pkl
  Features guardadas en: D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\models\features.json

Entrenamiento completado.
```

---

## 3. Modelo Afinado (Hyperparameter Tuning - Todas las Variables)

**Comando:** `D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\.venv\Scripts\python.exe D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\training_model_IEEE_CIS_tuned.py`

```text
D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\training_model_IEEE_CIS_tuned.py:84: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
  df["amt_ratio"] = df["TransactionAmt"] / (mean_amt + 1e-9)
D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\training_model_IEEE_CIS_tuned.py:88: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
  df["log_amt"] = np.log1p(df["TransactionAmt"])
==================================================
  Entrenamiento IEEE-CIS Fraud Detection
==================================================
[1/6] Cargando dataset limpio...
  Dataset limpio cargado : (590540, 360)
[2/6] Separando target y features...
  Columnas restantes para modelado: 358
[3/6] Feature engineering...
  Codificando 26 variables categ�ricas...
  Features finales: 360

  Train: (472432, 360) | Test: (118108, 360)
[4/6] Entrenando modelo Isolation Forest...
  Contamination estimada (tasa de fraude): 0.0350
[5/6] Evaluando modelo Isolation Forest...

  Latencia promedio: 0.9065 ms por transacci�n
  AUC-ROC : 0.7670
  F1-Score: 0.2597

  Reporte de clasificaci�n:
              precision    recall  f1-score   support

           0       0.97      0.97      0.97    113975
           1       0.26      0.26      0.26      4133

    accuracy                           0.95    118108
   macro avg       0.62      0.62      0.62    118108
weighted avg       0.95      0.95      0.95    118108


  Matriz de confusi�n:
[[110919   3056]
 [  3060   1073]]
[6/6] Guardando modelo y features...
  Modelo guardado en  : D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\models\model.pkl
  Features guardadas en: D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\models\features.json

Entrenamiento completado.
```

---

## 4. Modelo Definitivo (Tuning - Top 25 Variables)

**Comando:** `D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\.venv\Scripts\python.exe D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\training_model_IEEE_CIS_tuned_top25.py`

```text
============================================================
 Entrenamiento IEEE-CIS Fraud Detection - MODO TOP 25 VARIABLES
============================================================
[1/6] Cargando dataset limpio...
  Dataset limpio cargado : (590540, 360)
[2/6] Filtrando �nicamente las Top 25 variables predictivas...
  Variables seleccionadas (25): ['TransactionAmt', 'TransactionDT', 'ProductCD', 'card1', 'card4', 'card6', 'P_emaildomain', 'addr1', 'addr2', 'DeviceType', 'DeviceInfo', 'C1', 'C13', 'D1', 'V314', 'V201', 'V243', 'V257', 'C7', 'V242', 'V45', 'V246', 'V200', 'V258', 'C14']
[3/6] Feature engineering (Agregando ratios)...
  Features finales para entrenar: 27
[4/6] Entrenando Isolation Forest con Top 25 variables...
[5/6] Evaluando modelo...

  Latencia promedio: 0.1400 ms por transacci�n
  AUC-ROC : 0.7190
  F1-Score: 0.2941

  Reporte de clasificaci�n:
              precision    recall  f1-score   support

           0       0.97      0.97      0.97    113975
           1       0.29      0.29      0.29      4133

    accuracy                           0.95    118108
   macro avg       0.63      0.63      0.63    118108
weighted avg       0.95      0.95      0.95    118108


  Matriz de confusi�n:
[[111048   2927]
 [  2916   1217]]
[6/6] Guardando modelo y listado estricto de variables...
  Modelo guardado como: D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\models\model_top25.pkl
  Features est�ticas guardadas en: D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\models\features_top25.json

Entrenamiento Top 25 completado. Entregable listo para debatir.
```

---

## 5. Extracción de Parámetros Estadísticos (Entrada para PaySim)

**Comando:** `D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\.venv\Scripts\python.exe D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\extract_paysim_params.py`

```text
==================================================
Extracci�n de Par�metros Estad�sticos para PaySim
==================================================
[1/4] Cargando dataset limpio...
[2/4] Calculando desbalance de clase...
[3/4] Filtrando las 25 variables requeridas...
  -> Par�metros guardados en: D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\..\data_insight\paysim_params\paysim_statistical_params.json
[4/4] Calculando matriz de correlaci�n...
  -> Matriz de correlaci�n guardada en: D:\Onedrive\MAESTRIA\TFM\Desarrollo TFM\deteccion_fraude_tarjetas_credito\infraestructure_ieeecis\training\..\data_insight\paysim_params\paysim_correlation_matrix.csv

�Extracci�n completada con �xito!
Tu compa�ero puede utilizar los archivos en la carpeta 'infraestructure_ieeecis/data_insight/paysim_params' para configurar PaySim.
```

---

