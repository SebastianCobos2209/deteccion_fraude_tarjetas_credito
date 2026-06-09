"""
evaluate.py — Evaluación del PoC (solo para validación externa)
───────────────────────────────────────────────────────────────
Lee transactions_enriched de MongoDB y calcula métricas del modelo.
Corre INDEPENDIENTE del pipeline Spark, desde tu máquina local.

Uso:
    python evaluate.py
    python evaluate.py --threshold 0.5
    python evaluate.py --threshold 0.5 --min-samples 1000
"""

import argparse
import os
from pymongo import MongoClient
import numpy as np

# ── Conexión: igual que settings.py del pipeline ──────────────
# Desde tu máquina local usa localhost:27017 (puerto mapeado en docker-compose)
# Desde dentro de la red Docker usaría mongodb:27017
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:tfm2026@localhost:27017/fraude_db?authSource=admin"
)
MONGO_DB     = "fraude_db"
COL_ENRICHED = "transactions_enriched"


def main(threshold: float = 0.5, min_samples: int = 100):
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    try:
        client.admin.command("ping")
    except Exception as e:
        print(f"\n  [ERROR] No se pudo conectar a MongoDB: {e}")
        print(f"  Asegúrate de que el contenedor está corriendo: docker-compose up -d")
        return

    col  = client[MONGO_DB][COL_ENRICHED]
    docs = list(col.find(
        {"fraud_score": {"$ne": None}, "isFraud": {"$exists": True}},
        {"fraud_score": 1, "isFraud": 1, "_id": 0}
    ))

    if not docs:
        print("\n  No hay transacciones evaluables aún.")
        print("  Necesitas: MONGO_ENABLED=true en docker-compose y pipeline corriendo.")
        return

    if len(docs) < min_samples:
        print(f"\n  [WARN] Solo {len(docs)} muestras (mínimo recomendado: {min_samples}).")
        print(f"  Las métricas pueden no ser representativas.\n")

    y_true  = np.array([d["isFraud"]    for d in docs])
    y_score = np.array([d["fraud_score"] for d in docs])
    y_pred  = (y_score >= threshold).astype(int)

    from sklearn.metrics import (
        roc_auc_score, f1_score, recall_score,
        precision_score, confusion_matrix, classification_report
    )

    print(f"\n{'='*52}")
    print(f"  Evaluación del modelo — Isolation Forest")
    print(f"  Threshold : {threshold}  |  Muestras: {len(y_true)}")
    print(f"{'='*52}")
    print(f"  Fraudes reales    : {int(y_true.sum())} ({y_true.mean()*100:.2f}%)")
    print(f"  Alertas generadas : {int(y_pred.sum())} ({y_pred.mean()*100:.2f}%)")
    print(f"  Score medio       : {y_score.mean():.4f}")
    print(f"  Score máximo      : {y_score.max():.4f}")
    print()

    if len(np.unique(y_true)) < 2:
        print("  [WARN] Solo hay una clase en los datos.")
        print("  Necesitas transacciones fraudulentas (isFraud=1) para calcular AUC.")
        client.close()
        return

    auc = roc_auc_score(y_true, y_score)
    f1  = f1_score(y_true, y_pred, zero_division=0)
    rec = recall_score(y_true, y_pred, zero_division=0)
    pre = precision_score(y_true, y_pred, zero_division=0)

    print(f"  AUC-ROC   : {auc:.4f}  ({'bueno' if auc >= 0.75 else 'mejorable'})")
    print(f"  F1 Score  : {f1:.4f}")
    print(f"  Recall    : {rec:.4f}  (fraudes detectados del total real)")
    print(f"  Precision : {pre:.4f}  (alertas correctas del total alertado)")
    print()

    cm = confusion_matrix(y_true, y_pred)
    print("  Matriz de confusión:")
    print(f"    TN={cm[0,0]:5d}  FP={cm[0,1]:5d}   (legítimas)")
    print(f"    FN={cm[1,0]:5d}  TP={cm[1,1]:5d}   (fraudes)")
    print()
    print("  Reporte completo:")
    print(classification_report(
        y_true, y_pred,
        target_names=["Legítima", "Fraude"],
        zero_division=0,
    ))

    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evalúa el modelo de detección de fraude")
    parser.add_argument("--threshold",   type=float, default=0.5,
                        help="Umbral de clasificación (default: 0.5)")
    parser.add_argument("--min-samples", type=int,   default=100,
                        help="Mínimo de muestras recomendado (default: 100)")
    args = parser.parse_args()
    main(args.threshold, args.min_samples)