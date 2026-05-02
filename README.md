#Real-Time Fraud Detection System

Sistema de detección de fraude financiero en tiempo real utilizando Kafka, Spark y Machine Learning.

---

## Description

Este proyecto implementa una arquitectura de streaming para detectar fraude en transacciones financieras en tiempo real.

Combina:
- Entrenamiento de modelos con datos históricos
- Simulación de transacciones
- Procesamiento distribuido
- Predicción en tiempo real

## Architecture

El sistema sigue una arquitectura de procesamiento en tiempo real dividida en dos fases:

### 1. Entrenamiento (Offline)
- Se utiliza el dataset IEEE-CIS Fraud Detection
- Se entrena un modelo de Machine Learning
- El modelo se guarda para uso en producción
---
### 2. Procesamiento en tiempo real

- PaySim genera transacciones simuladas
- Kafka actúa como sistema de mensajería
- Spark procesa los datos en streaming
- Se aplican predicciones de fraude
- Los resultados se almacenan en la base de datos

### Flujo completo

            +----------------------+
            |  IEEE Dataset        |
            | (Model Training)     |
            +----------+-----------+
                       |
                       v
                +-------------+
                |  ML Model   |
                +------+------+
                       |
                       v
    +------------+ +--------------+ +----------------+ +-------------+
    | PaySim | --> | Kafka    | --> | Spark      | --> | Database |
    | (Simulator)| | (Streaming)  | | (Processing) |   |          |
    +------------+ +--------------+ +----------------+ +-------------+
## Datasets

- IEEE-CIS Fraud Detection → entrenamiento del modelo
- PaySim → simulación de transacciones en tiempo real

---
---

## Technologies

- Python
- Apache Kafka
- Apache Spark (Structured Streaming)
- Docker
- Scikit-learn / XGBoost
- PostgreSQL

---

## Installation

Clona el repositorio:

```bash
git clone <repo-url>
cd project
docker-compose up -d
execute .bat(windows) o sh(linux)
