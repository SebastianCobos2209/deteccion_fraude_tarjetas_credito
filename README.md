# Real-Time Fraud Detection System

Sistema de detección de fraude financiero en tiempo real utilizando Kafka, Spark Structured Streaming y Machine Learning.

Combina entrenamiento offline con el dataset IEEE-CIS, simulación de transacciones en vivo y procesamiento distribuido con alertas en Kafka y MongoDB.

---

## Tabla de contenidos

1. [Infraestructura IEEE-CIS y modelos](#1-infraestructura-ieee-cis-y-modelos)
2. [Producers: arquitectura y funcionamiento](#2-producers-arquitectura-y-funcionamiento)
3. [Infraestructura Spark y ejecución del sistema](#3-infraestructura-spark-y-ejecución-del-sistema)

---

## 1. Infraestructura IEEE-CIS y modelos

Módulo offline ubicado en `infraestructure_ieeecis/`. Prepara los datos históricos, entrena modelos de detección de anomalías y genera los artefactos que alimentan el simulador y el pipeline de Spark.

### 1.1 Estructura del módulo

```
infraestructure_ieeecis/
├── notebooks/
│   └── eda_ieeecis.ipynb          # EDA y limpieza → train_cleaned.csv
├── training/
│   ├── confiig.py                 # Rutas centralizadas (data/, models/)
│   ├── training_model_IEEE_CIS.py           # Modelo base (todas las variables)
│   ├── training_model_IEEE_CIS_tuned.py     # Modelo afinado (hyperparameter tuning)
│   ├── training_model_IEEE_CIS_tuned_top15.py
│   ├── training_model_IEEE_CIS_tuned_top25.py  # Modelo definitivo (Top 25)
│   ├── find_top_features.py       # Selección de variables relevantes
│   ├── optimize_top15.py
│   └── extract_paysim_params.py   # Parámetros estadísticos para el producer
├── database/
│   ├── populate_static_data.py    # user_profiles + cards en MongoDB
│   └── populate_dynamic_data.py   # transacciones históricas en MongoDB
└── run_all_experiments.py         # Orquestador de todo el pipeline offline
```

### 1.2 Flujo de trabajo offline

| Paso | Componente | Descripción |
|------|-----------|-------------|
| 1 | `eda_ieeecis.ipynb` | Análisis exploratorio y limpieza del dataset IEEE-CIS. Genera `data/ieee/train_cleaned.csv`. |
| 2 | `training_model_IEEE_CIS.py` | Entrena Isolation Forest con todas las variables disponibles (baseline). |
| 3 | `training_model_IEEE_CIS_tuned.py` | Afinación de hiperparámetros sobre el conjunto completo. |
| 4 | `training_model_IEEE_CIS_tuned_top25.py` | **Modelo de producción**: Isolation Forest con las 25 variables definitivas (Top 15 + 10 de red/comportamiento). |
| 5 | `extract_paysim_params.py` | Extrae medias, varianzas, frecuencias categóricas y matriz de correlación → `data/paysim_params/`. |

El script `run_all_experiments.py` ejecuta los pasos 1–5 de forma secuencial y genera un reporte en `doc/Reporte_Resultados_Entrenamientos.md`.

### 1.3 Carpeta `models/`

Los artefactos entrenados se guardan en `models/` (excluida de git):

| Archivo | Descripción |
|---------|-------------|
| `model.pkl` / `model_top25.pkl` | Modelo Isolation Forest serializado con joblib |
| `features.json` | Lista de variables usadas en entrenamiento |
| `isolation_forest.joblib` | Modelo montado en el contenedor Spark (`/opt/spark_models/`) |
| `scaler.joblib` | Escalador asociado al modelo |

El pipeline de Spark carga el modelo desde `/opt/spark_models/` (volumen Docker `../models`). Si no existe un modelo en disco, Spark entrena uno en caliente durante la fase de **warmup** con las transacciones recibidas.

### 1.4 Variables del modelo definitivo (Top 25)

**Numéricas (19):** TransactionAmt, TransactionDT, card1, addr1, addr2, C1, C13, D1, V314, V201, V243, V257, C7, V242, V45, V246, V200, V258, C14.

**Categóricas (6):** ProductCD, card4, card6, P_emaildomain, DeviceType, DeviceInfo.

Estas mismas variables son las que genera el producer y consume Spark en streaming.

### 1.5 Cómo ejecutar la infraestructura IEEE-CIS

**Prerrequisitos:** Python 3.10+, dataset IEEE-CIS en `data/ieee/` (`train_transaction.csv`, `train_identity.csv`).

```bash
# 1. Instalar dependencias
cd infraestructure_ieeecis
pip install -r requirements.txt

# 2. Ejecutar todo el pipeline offline (recomendado)
python run_all_experiments.py
```

**Ejecución paso a paso (manual):**

```bash
# EDA y limpieza
jupyter nbconvert --execute --inplace notebooks/eda_ieeecis.ipynb

# Entrenamientos
python training/training_model_IEEE_CIS.py
python training/training_model_IEEE_CIS_tuned.py
python training/training_model_IEEE_CIS_tuned_top25.py

# Parámetros para el simulador
python training/extract_paysim_params.py
```

**Población opcional de MongoDB con datos históricos** (requiere Docker con MongoDB activo):

```bash
python database/populate_static_data.py    # user_profiles + cards
python database/populate_dynamic_data.py   # transacciones históricas
```

---

## 2. Producers: arquitectura y funcionamiento

Módulo ubicado en `producers/`. Simula el sistema bancario **Vertexon** generando usuarios, tarjetas y transacciones sintéticas con la distribución estadística del dataset IEEE-CIS, y las publica en Kafka.

### 2.1 Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                        ProducerFactory                          │
│  Construye el grafo de dependencias desde config y CLI args     │
└────────────────────────────┬────────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌──────────────────────┐
│  VertexonClient │  │  Builders       │  │  KafkaEventPublisher │
│  (HTTP / Mock)  │  │  Usuario        │  │  → vertexon.usuarios │
│                 │  │  Tarjeta        │  │  → vertexon.tarjetas │
│  get_customer() │  │  Transaccion    │  │  → transactions.raw  │
│  get_card_txs() │  └────────┬────────┘  └──────────────────────┘
│  get_card_rsa() │           │
└─────────────────┘           │
                    ┌─────────┴─────────┐
                    ▼                   ▼
           ┌──────────────┐    ┌──────────────────┐
           │ IEEEParameters│    │ FraudLabeler     │
           │ + Correlated  │    │ (ground truth    │
           │   Generator   │    │  isFraud)        │
           │ + Categorical │    └──────────────────┘
           │   Sampler     │
           └──────────────┘
                             │
                    ┌────────▼────────┐
                    │ ProducerService │
                    │  loop infinito  │
                    └─────────────────┘
```

### 2.2 Capas del módulo

| Capa | Carpeta | Responsabilidad |
|------|---------|-----------------|
| **Dominio** | `domain/` | Entidades (`Usuario`, `Tarjeta`, `Transaccion`) y `FraudLabeler` (etiqueta `isFraud` con tasa de contaminación IEEE ~3.5%). |
| **Estadística** | `statistics/` | `IEEEParameters` (distribuciones numéricas y categóricas), `CorrelatedNumericGenerator` (Cholesky 19×19), `CategoricalSampler`. |
| **Builders** | `builders/` | Construyen entidades a partir de datos Vertexon + generadores estadísticos. |
| **Infraestructura** | `infrastructure/` | `VertexonHttpClient` (SwaggerHub mock) con fallback a `MockVertexonClient`; `KafkaEventPublisher`. |
| **Servicios** | `services/` | `ProducerService` — orquesta el ciclo de generación y publicación. |
| **Config** | `config/settings.py` | Broker Kafka, topics, intervalo, variación por ciclo. |

### 2.3 Ciclo de funcionamiento

En cada ciclo (por defecto cada **2 segundos**):

1. Obtiene datos base del mock Vertexon (cliente, transacciones de referencia, RSA de tarjeta).
2. Genera `variacion` usuarios (default: **5** por ciclo).
3. Por cada usuario: construye perfil → tarjeta asociada → transacción con 25 features IEEE.
4. Etiqueta la transacción con `isFraud` (ground truth para evaluación).
5. Publica los tres eventos en Kafka con clave = ID de entidad.

### 2.4 Topics Kafka generados

| Topic | Contenido | Retención |
|-------|-----------|-----------|
| `vertexon.usuarios` | Perfiles de usuario | 90 días (compactado) |
| `vertexon.tarjetas` | Datos de tarjeta (CVV encriptado) | 90 días (compactado) |
| `transactions.raw` | Transacciones con 25 features + `isFraud` | 7 días, 3 particiones |

### 2.5 Cómo ejecutar el producer

**Prerrequisitos:** Kafka activo (Docker), topics creados (ver sección 3).

```bash
# Instalar dependencias
cd producers
pip install -r requirements.txt

# Ejecución con valores por defecto (2s entre ciclos, 5 usuarios/ciclo)
python main.py

# Con parámetros personalizados
python main.py --intervalo 2 --variacion 5
```

**Variables de entorno opcionales:**

| Variable | Default | Descripción |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | Broker Kafka |
| `INTERVALO` | `2.0` | Segundos entre ciclos |
| `VARIACION` | `5` | Usuarios generados por ciclo |
| `ENCRYPTION_KEY` | auto-generada | Clave Fernet para CVV |

Detener con `Ctrl+C`. El producer imprime un resumen de transacciones y tasa de fraude al finalizar.

---

## 3. Infraestructura Spark y ejecución del sistema

Módulo ubicado en `spark/`. Consume los tres topics de Kafka, enriquece transacciones, aplica Isolation Forest en streaming, persiste resultados en MongoDB y publica alertas.

### 3.1 Arquitectura de infraestructura Docker

```
                    docker-compose.yml
┌──────────────────────────────────────────────────────────────────┐
│  Zookeeper (2181)  ←→  Kafka (9092 / 29092)  ←→  Kafka UI (8080)│
│                                                                  │
│  MongoDB (27017)  ←→  Mongo Express (8081)  ←→  Grafana (3000)  │
│                                                                  │
│  Spark Master (7077, UI:4040)  ←→  Spark Worker (2G / 2 cores)  │
│         ↑                                                        │
│  Spark Submit (spark/main.py) — arranca automáticamente         │
└──────────────────────────────────────────────────────────────────┘
```

| Servicio | Puerto | Acceso |
|----------|--------|--------|
| Kafka | 9092 | `localhost:9092` |
| Kafka UI | 8080 | http://localhost:8080 |
| MongoDB | 27017 | `admin` / `tfm2026` |
| Mongo Express | 8081 | http://localhost:8081 |
| Spark Web UI | 4040 | http://localhost:4040 |
| Grafana | 3000 | `admin` / `admin` |

### 3.2 Arquitectura del pipeline Spark

```
Kafka                          Spark Structured Streaming
─────────                      ──────────────────────────
vertexon.usuarios  ──→  Stream USR  ──→  MongoDB: user_profiles
vertexon.tarjetas  ──→  Stream TAR  ──→  MongoDB: cards
transactions.raw   ──→  Stream TXS  ──→  BatchProcessor
                                              │
                    ┌─────────────────────────┼─────────────────────────┐
                    ▼                         ▼                         ▼
              WarmupOrchestrator      RetrainingOrchestrator      AlertEvaluator
              (primeras N txs)        (ventana deslizante)        (threshold 0.5)
                    │                         │                         │
                    └──────────── ModelRegistry (Isolation Forest) ─────┘
                                              │
                    ┌─────────────────────────┼─────────────────────────┐
                    ▼                         ▼                         ▼
           MongoDB: transactions_raw   transactions_enriched      fraud_alerts
           Kafka: fraud.alerts
```

**Capas del módulo Spark:**

| Capa | Carpeta | Responsabilidad |
|------|---------|-----------------|
| **Pipeline** | `pipeline/` | `PipelineFactory` (DI), `StreamingPipeline` (3 streams paralelos). |
| **Processors** | `processors/` | `BatchProcessor` — procesa micro-batches de transacciones. |
| **Services** | `services/` | Warmup, reentrenamiento periódico, feature engineering, evaluación de alertas. |
| **Models AI** | `models_ai/` | `IsolationForestTrainer`, `ModelRegistry`, buffers, normalizador sigmoide. |
| **Infrastructure** | `infrastructure/` | Kafka alert publisher, repositorios MongoDB, `SparkSessionFactory`. |
| **Schemas** | `schemas/` | Esquemas Spark para transacciones, usuarios y tarjetas. |

### 3.3 Flujo de una transacción

1. **Stream TXS** consume `transactions.raw` (micro-batch cada 5 s, máx. 200 offsets).
2. Persiste el raw en MongoDB (`transactions_raw`).
3. Enriquece con perfil de usuario y tarjeta desde MongoDB.
4. **Warmup:** acumula las primeras `WARMUP_SIZE` transacciones (default 2000 en Docker) y entrena el modelo inicial.
5. **Scoring:** extrae 19 features numéricas, calcula score con Isolation Forest, normaliza con sigmoide (0–1).
6. Si score ≥ `FRAUD_THRESHOLD` (0.5) → alerta en Kafka (`fraud.alerts`) y MongoDB (`fraud_alerts`).
7. **Reentrenamiento:** cada `RETRAIN_INTERVAL` segundos (300) reentrena con ventana deslizante.

### 3.4 Colecciones MongoDB

| Colección | Origen |
|-----------|--------|
| `user_profiles` | Stream USR |
| `cards` | Stream TAR |
| `transactions_raw` | BatchProcessor |
| `transactions_enriched` | BatchProcessor (tx + score + features) |
| `fraud_alerts` | BatchProcessor (solo alertas) |
| `model_metrics` | Métricas de reentrenamiento |

### 3.5 Cómo levantar y ejecutar todo el sistema

#### Paso 1 — Infraestructura Docker

```bash
cd docker
docker-compose up -d
```

Esperar a que Kafka y MongoDB estén healthy (`docker-compose ps`).

#### Paso 2 — Crear topics Kafka

**Windows:**
```bash
cd topics_scripts
topics.bat
```

**Linux / macOS:**
```bash
cd topics_scripts
chmod +x topics.sh
./topics.sh
```

Topics creados: `vertexon.usuarios`, `vertexon.tarjetas`, `transactions.raw`, `transactions.enriched`, `fraud.scores`, `fraud.alerts`, `transactions.dlq`.

#### Paso 3 — Spark (automático con Docker)

El servicio `spark-submit` en `docker-compose.yml` arranca `spark/main.py` automáticamente al levantar la infraestructura. No requiere acción manual si usas Docker.

**Ejecución manual de Spark** (fuera de Docker o para desarrollo):

```bash
cd spark
pip install -r requirements.txt

# Con Spark local (requiere Spark instalado)
export KAFKA_BROKER=localhost:9092
export MONGO_ENABLED=true
export MONGO_URI="mongodb://admin:tfm2026@localhost:27017/fraude_db?authSource=admin"
export SPARK_MASTER=local[*]

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  main.py
```

#### Paso 4 — Producer

Con Kafka y topics listos, en otra terminal:

```bash
cd producers
pip install -r requirements.txt
python main.py
```

#### Paso 5 — Verificar resultados

- **Kafka UI:** http://localhost:8080 — mensajes en los topics.
- **Mongo Express:** http://localhost:8081 — colecciones pobladas.
- **Grafana:** http://localhost:3000 — dashboard en tiempo real.
- **Spark UI:** http://localhost:4040 — jobs y streams activos.

### 3.6 Variables de entorno Spark (Docker)

| Variable | Default (Docker) | Descripción |
|----------|------------------|-------------|
| `KAFKA_BROKER` | `kafka:29092` | Broker interno Docker |
| `MONGO_ENABLED` | `true` | Persistencia en MongoDB |
| `MONGO_URI` | ver docker-compose | URI de conexión |
| `SPARK_MASTER` | `spark://spark-master:7077` | Master del cluster |
| `WARMUP_SIZE` | `2000` | Transacciones antes del primer entrenamiento |
| `FRAUD_THRESHOLD` | `0.5` | Umbral de alerta (0–1) |
| `RETRAIN_INTERVAL` | `300` | Segundos entre reentrenamientos |

### 3.7 Orden de ejecución recomendado

```
1. docker-compose up -d          → Infraestructura (Kafka, MongoDB, Spark)
2. topics.bat / topics.sh        → Crear topics Kafka
3. (opcional) run_all_experiments.py  → Entrenar modelos offline
4. spark-submit (automático)     → Pipeline de streaming
5. python producers/main.py      → Generar transacciones
6. Grafana / Mongo Express       → Monitorear resultados
```

---

## Tecnologías

- **Python** 3.10+
- **Apache Kafka** 7.5 (Confluent)
- **Apache Spark** 3.5.1 (Structured Streaming)
- **MongoDB** 7.0
- **Scikit-learn** (Isolation Forest)
- **Docker / Docker Compose**
- **Grafana** 10.4

## Datasets

| Dataset | Uso |
|---------|-----|
| IEEE-CIS Fraud Detection | Entrenamiento offline y extracción de parámetros estadísticos |
| Vertexon Mock (SwaggerHub) | Plantilla de entidades bancarias para el producer |
