"""
evaluate.py — Evaluación del PoC
Uso:
    python evaluate.py
    python evaluate.py --threshold 0.5
    python evaluate.py --threshold 0.45 --min-samples 500
"""

from __future__ import annotations

import argparse
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import numpy as np


# ══════════════════════════════════════════════════════════════
# 1. CONFIG  —  único lugar donde viven los parámetros
# ══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class EvaluationConfig:
    """
    Parámetros de evaluación.
    frozen=True → inmutable después de crearse (no hay estado oculto).
    """
    mongo_uri:     str   = field(default_factory=lambda: os.getenv(
        "MONGO_URI",
        "mongodb://admin:tfm2026@localhost:27017/fraude_db?authSource=admin",
    ))
    mongo_db:      str   = "fraude_db"
    collection:    str   = "transactions_enriched"  # transactions_enriched
    col_metrics:   str   = "model_metrics"           # donde se guardan los resultados
    threshold:     float = 0.4
    min_samples:   int   = 100
    model_version: str   = "IsolationForest_Top25_v1.0"
    contamination: float = 0.035
    skip_warmup:   bool  = True   # excluir txs del warm-up (is_warmed_up=False)


# ══════════════════════════════════════════════════════════════
# 2. REPOSITORIO  —  interfaz + implementación MongoDB
#    DIP: EvaluationService depende de la abstracción, no de pymongo
# ══════════════════════════════════════════════════════════════

@dataclass
class TransactionRecord:
    """Un registro leído de la base de datos."""
    fraud_score: float
    is_fraud:    int


class TransactionRepository(ABC):
    """
    Interfaz de acceso a datos.
    OCP: para cambiar la fuente (Postgres, CSV, mock) solo se
    implementa esta interfaz — EvaluationService no cambia.
    """

    @abstractmethod
    def fetch_scored_transactions(self) -> List[TransactionRecord]:
        """Retorna todas las transacciones que ya tienen fraud_score."""
        ...

    @abstractmethod
    def ping(self) -> bool:
        """Verifica que la conexión está activa."""
        ...


class MongoTransactionRepository(TransactionRepository):
    """
    Implementación concreta para MongoDB.
    SRP: solo sabe leer datos de Mongo, no calcula ni imprime nada.
    """

    def __init__(self, config: EvaluationConfig) -> None:
        from pymongo import MongoClient
        self._config     = config
        self._client     = MongoClient(
            config.mongo_uri,
            serverSelectionTimeoutMS=5000,
        )
        self._collection = self._client[config.mongo_db][config.collection]

    def ping(self) -> bool:
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False

    def fetch_scored_transactions(self) -> List[TransactionRecord]:
        # Filtro base: solo txs con fraud_score calculado e isFraud conocido
        query: dict = {
            "fraud_score": {"$ne": None},
            "isFraud":     {"$exists": True},
        }

        # skip_warmup=True → excluir las txs del período de warm-up.
        # Durante el warm-up el modelo aún no tiene datos frescos:
        # is_warmed_up=False en esos documentos.
        # Evaluarlas daría AUC=1.0 por overfitting (el modelo vio
        # esos datos al entrenar). Solo evaluar txs post-warm-up
        # da métricas representativas del comportamiento real.
        if self._config.skip_warmup:
            query["is_warmed_up"] = True

        cursor = self._collection.find(
            query,
            {"fraud_score": 1, "isFraud": 1, "_id": 0},
        )
        return [
            TransactionRecord(
                fraud_score=float(doc["fraud_score"]),
                is_fraud=int(doc["isFraud"]),
            )
            for doc in cursor
        ]

    def close(self) -> None:
        self._client.close()


# ══════════════════════════════════════════════════════════════
# 2b. REPOSITORIO DE METRICAS
# ══════════════════════════════════════════════════════════════

class MetricsRepository(ABC):
    @abstractmethod
    def save(self, result, config) -> None: ...


class MongoMetricsRepository(MetricsRepository):
    def save(self, result, config) -> None:
        if result.auc_roc is None:
            print("  [INFO] Sin ambas clases — metricas no guardadas en model_metrics.")
            return
        try:
            from datetime import datetime
            from pymongo import MongoClient
            client = MongoClient(config.mongo_uri, serverSelectionTimeoutMS=5000)
            cm = result.confusion_matrix
            client[config.mongo_db][config.col_metrics].insert_one({
                "evaluated_at":       datetime.utcnow(),
                "samples_evaluated":  result.n_samples,
                "threshold":          result.threshold,
                "auc_roc":            result.auc_roc,
                "f1_score":           result.f1,
                "recall":             result.recall,
                "precision":          result.precision,
                "true_positives":     int(cm[1, 1]),
                "false_positives":    int(cm[0, 1]),
                "true_negatives":     int(cm[0, 0]),
                "false_negatives":    int(cm[1, 0]),
                "model_version":      config.model_version,
                "contamination_rate": config.contamination,
            })
            print(f"  Metricas guardadas en '{config.col_metrics}'.")
            client.close()
        except Exception as e:
            print(f"  [WARN] No se pudieron guardar metricas: {e}")


# ══════════════════════════════════════════════════════════════
# 3. RESULTADO  —  contenedor de metricas
#    SRP: solo almacena, no calcula ni imprime
# ══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class EvaluationResult:
    """Métricas calculadas. Inmutable para evitar modificaciones accidentales."""
    n_samples:       int
    n_real_frauds:   int
    n_alerts:        int
    fraud_rate:      float
    score_mean:      float
    score_max:       float
    threshold:       float
    auc_roc:         Optional[float]
    f1:              Optional[float]
    recall:          Optional[float]
    precision:       Optional[float]
    confusion_matrix: Optional[np.ndarray]
    report:          Optional[str]
    warning:         Optional[str] = None


# ══════════════════════════════════════════════════════════════
# 4. CALCULADORA  —  lógica de métricas pura
#    SRP: solo calcula, no lee datos ni imprime
#    Funciones puras → fáciles de testear unitariamente
# ══════════════════════════════════════════════════════════════

class MetricsCalculator:
    """
    Calcula métricas de clasificación a partir de arrays numpy.
    No tiene estado interno — todos los métodos son estáticos.
    """

    @staticmethod
    def build_arrays(
        records: List[TransactionRecord],
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Convierte lista de registros a arrays y_true / y_score."""
        y_true  = np.array([r.is_fraud    for r in records])
        y_score = np.array([r.fraud_score for r in records])
        return y_true, y_score

    @staticmethod
    def calculate(
        records:   List[TransactionRecord],
        threshold: float,
    ) -> EvaluationResult:
        from sklearn.metrics import (
            roc_auc_score, f1_score, recall_score,
            precision_score, confusion_matrix, classification_report,
        )

        y_true, y_score = MetricsCalculator.build_arrays(records)
        y_pred = (y_score >= threshold).astype(int)

        has_both_classes = len(np.unique(y_true)) > 1
        warning = None if has_both_classes else (
            "Solo hay una clase en los datos. "
            "Necesitas transacciones con isFraud=1 para calcular AUC."
        )

        return EvaluationResult(
            n_samples       = len(records),
            n_real_frauds   = int(y_true.sum()),
            n_alerts        = int(y_pred.sum()),
            fraud_rate      = float(y_true.mean()),
            score_mean      = float(y_score.mean()),
            score_max       = float(y_score.max()),
            threshold       = threshold,
            auc_roc         = float(roc_auc_score(y_true, y_score))   if has_both_classes else None,
            f1              = float(f1_score(y_true, y_pred,          zero_division=0)) if has_both_classes else None,
            recall          = float(recall_score(y_true, y_pred,      zero_division=0)) if has_both_classes else None,
            precision       = float(precision_score(y_true, y_pred,   zero_division=0)) if has_both_classes else None,
            confusion_matrix= confusion_matrix(y_true, y_pred)        if has_both_classes else None,
            report          = classification_report(
                                  y_true, y_pred,
                                  target_names=["Legítima", "Fraude"],
                                  zero_division=0,
                              ) if has_both_classes else None,
            warning         = warning,
        )


# ══════════════════════════════════════════════════════════════
# 5. REPORTER  —  presentación de resultados
#    SRP: solo sabe imprimir, no calcula ni lee datos
#    OCP: para añadir JSON/CSV reporter se hereda esta clase
# ══════════════════════════════════════════════════════════════

class EvaluationReporter:
    """Imprime EvaluationResult en consola."""

    def report(self, result: EvaluationResult) -> None:
        self._print_header(result)
        if result.warning:
            print(f"  [WARN] {result.warning}\n")
            return
        self._print_metrics(result)
        self._print_confusion_matrix(result)
        self._print_full_report(result)

    def _print_header(self, r: EvaluationResult) -> None:
        print(f"\n{'='*52}")
        print(f"  Evaluación del modelo — Isolation Forest")
        print(f"  Threshold : {r.threshold}  |  Muestras : {r.n_samples}")
        print(f"{'='*52}")
        print(f"  Fraudes reales    : {r.n_real_frauds} ({r.fraud_rate*100:.2f}%)")
        print(f"  Alertas generadas : {r.n_alerts} ({r.n_alerts/r.n_samples*100:.2f}%)")
        print(f"  Score medio       : {r.score_mean:.4f}")
        print(f"  Score máximo      : {r.score_max:.4f}")
        print()

    def _print_metrics(self, r: EvaluationResult) -> None:
        auc_label = "bueno" if r.auc_roc >= 0.75 else "mejorable"
        print(f"  AUC-ROC   : {r.auc_roc:.4f}  ({auc_label})")
        print(f"  F1 Score  : {r.f1:.4f}")
        print(f"  Recall    : {r.recall:.4f}  (fraudes detectados del total real)")
        print(f"  Precision : {r.precision:.4f}  (alertas correctas del total alertado)")
        print()

    def _print_confusion_matrix(self, r: EvaluationResult) -> None:
        cm = r.confusion_matrix
        print("  Matriz de confusión:")
        print(f"    TN={cm[0,0]:5d}  FP={cm[0,1]:5d}   (legítimas)")
        print(f"    FN={cm[1,0]:5d}  TP={cm[1,1]:5d}   (fraudes)")
        print()

    def _print_full_report(self, r: EvaluationResult) -> None:
        print("  Reporte completo:")
        print(r.report)


# ══════════════════════════════════════════════════════════════
# 6. SERVICE  —  orquestador
#    SRP: coordina las capas, no implementa ninguna
#    DIP: recibe TransactionRepository (interfaz), no MongoClient
# ══════════════════════════════════════════════════════════════

class EvaluationService:
    """
    Orquesta el flujo completo:
      repositorio → calculadora → reporter

    No conoce MongoDB, pymongo ni sklearn directamente.
    Recibe sus dependencias por inyección → testeable con mocks.
    """

    def __init__(
        self,
        repository:         TransactionRepository,
        calculator:         MetricsCalculator,
        reporter:           EvaluationReporter,
        metrics_repository: MetricsRepository,
        config:             EvaluationConfig,
    ) -> None:
        self._repository         = repository
        self._calculator         = calculator
        self._reporter           = reporter
        self._metrics_repository = metrics_repository
        self._config             = config

    def run(self) -> None:
        if not self._repository.ping():
            print(
                "\n  [ERROR] No se pudo conectar a MongoDB.\n"
                "  Asegúrate de que el contenedor está corriendo:\n"
                "    docker-compose up -d\n"
            )
            return

        records = self._repository.fetch_scored_transactions()

        if not records:
            print(
                "\n  No hay transacciones evaluables aún.\n"
                "  Necesitas MONGO_ENABLED=true en docker-compose.\n"
            )
            return

        if len(records) < self._config.min_samples:
            print(
                f"\n  [WARN] Solo {len(records)} muestras "
                f"(mínimo recomendado: {self._config.min_samples}).\n"
                f"  Las métricas pueden no ser representativas.\n"
            )

        result = self._calculator.calculate(records, self._config.threshold)
        self._reporter.report(result)
        self._metrics_repository.save(result, self._config)


# ══════════════════════════════════════════════════════════════
# 7. ENTRY POINT  —  composición de dependencias
#    Único lugar que conoce las implementaciones concretas
# ══════════════════════════════════════════════════════════════

def build_service(config: EvaluationConfig) -> EvaluationService:
    """
    Construye el grafo de dependencias.
    Si mañana cambias a PostgreSQL, solo cambias esta función.
    """
    return EvaluationService(
        repository         = MongoTransactionRepository(config),
        calculator         = MetricsCalculator(),
        reporter           = EvaluationReporter(),
        metrics_repository = MongoMetricsRepository(),
        config             = config,
    )


def parse_args() -> EvaluationConfig:
    parser = argparse.ArgumentParser(
        description="Evalúa el modelo de detección de fraude"
    )
    parser.add_argument(
        "--threshold", type=float, default=0.5,
        help="Umbral de clasificación (default: 0.5)",
    )
    parser.add_argument(
        "--min-samples", type=int, default=100,
        help="Minimo de muestras recomendado (default: 100)",
    )
    parser.add_argument(
        "--model-version", type=str, default="IsolationForest_Top25_v1.0",
        help="Version del modelo para registrar en model_metrics",
    )
    parser.add_argument(
        "--include-warmup", action="store_true", default=False,
        help="Incluir txs del warm-up en la evaluacion (no recomendado — genera AUC=1.0)",
    )
    args = parser.parse_args()
    return EvaluationConfig(
        threshold     = args.threshold,
        min_samples   = args.min_samples,
        model_version = args.model_version,
        skip_warmup   = not args.include_warmup,
    )


if __name__ == "__main__":
    config  = parse_args()
    service = build_service(config)
    service.run()