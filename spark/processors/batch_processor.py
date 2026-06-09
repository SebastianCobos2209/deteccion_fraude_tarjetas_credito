"""
processors/batch_processor.py
"""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from config.settings import COL_ENRICHED, COL_ALERTS, WARMUP_SIZE
from infrastructure.contracts.alert_publisher     import AlertPublisher
from infrastructure.contracts.document_repository import DocumentRepository
from models_ai.model_registry                     import ModelRegistry
from services.alert_evaluator                     import AlertEvaluator
from services.feature_engineering                 import (
    extraer_features,
    calcular_features_engineered,
)
from services.warmup_orchestrator      import WarmupOrchestrator
from services.retraining_orchestrator  import RetrainingOrchestrator
from utils.helpers import ahora_iso
from utils.logger  import get_logger

logger = get_logger("BatchProcessor")


class BatchProcessor:

    def __init__(
        self,
        registry:     ModelRegistry,
        warmup:       WarmupOrchestrator,
        retraining:   RetrainingOrchestrator,
        evaluator:    AlertEvaluator,
        publisher:    AlertPublisher,
        repository:   DocumentRepository,
    ) -> None:
        self._registry   = registry
        self._warmup     = warmup
        self._retraining = retraining
        self._evaluator  = evaluator
        self._publisher  = publisher
        self._repository = repository

    # ── Punto de entrada público ──────────────────────────

    def process(self, df: DataFrame, epoch_id: int) -> None:
        """
        Args:
            df:       DataFrame con las transacciones del batch
            epoch_id: identificador secuencial del batch
        """
        if df.rdd.isEmpty():
            return

        rows     = df.collect()
        enriched: List[dict] = []
        alertas:  List[dict] = []

        for row in rows:
            tx = row.asDict()
            self._process_transaction(tx, epoch_id, enriched, alertas)

        self._repository.save(COL_ENRICHED, enriched)
        self._repository.save(COL_ALERTS,   alertas)
        self._publisher.publish(df.sparkSession, alertas)

        self._log_batch(epoch_id, rows, alertas)

        self._retraining.maybe_retrain()


    def _process_transaction(
        self,
        tx:       dict,
        epoch_id: int,
        enriched: List[dict],
        alertas:  List[dict],
    ) -> None:
        """Ejecuta los pasos 2a-2f para una transacción."""

        features     = extraer_features(tx)
        features_eng = calcular_features_engineered(tx)

        self._warmup.notify(features.tolist())

        self._retraining.notify(features.tolist())

        fraud_score = (
            self._registry.score(features)
            if self._registry.is_ready
            else -1.0
        )

        enriched.append(
            self._build_enriched(tx, features_eng, fraud_score, epoch_id)
        )

        if self._registry.is_warmed_up and self._evaluator.is_suspicious(fraud_score):
            alertas.append(
                self._build_alert(tx, features_eng, fraud_score)
            )

    def _build_enriched(
        self,
        tx:          dict,
        features_eng: dict,
        fraud_score:  float,
        epoch_id:     int,
    ) -> dict:
        return {
            **tx,
            **features_eng,
            "fraud_score":   round(fraud_score, 4) if fraud_score >= 0 else None,
            "is_suspicious": (
                self._evaluator.is_suspicious(fraud_score)
                if fraud_score >= 0 else False
            ),
            "is_warmed_up":  self._registry.is_warmed_up,
            "processed_at":  ahora_iso(),
            "epoch_id":      epoch_id,
        }

    def _build_alert(
        self,
        tx:           dict,
        features_eng: dict,
        fraud_score:  float,
    ) -> dict:
        return {
            "transaccionID":  tx.get("transaccionID"),
            "usuarioID":      tx.get("usuarioID"),
            "tarjetaID":      tx.get("tarjetaID"),
            "TransactionAmt": tx.get("TransactionAmt"),
            "fraud_score":    round(fraud_score, 4),
            "zscore_amt":     features_eng.get("zscore_amt"),
            "velocity":       features_eng.get("velocity"),
            "isFraud_label":  tx.get("isFraud"),
            "alerted_at":     ahora_iso(),
        }
    
    def _log_batch(
        self,
        epoch_id: int,
        rows:     list,
        alertas:  List[dict],
    ) -> None:
        logger.info(
            f"[BATCH {epoch_id:04d}] "
            f"txs={len(rows)} | "
            f"is_ready={self._registry.is_ready} | "
            f"is_warmed_up={self._registry.is_warmed_up} | "
            f"warmup={self._warmup.progress} | "
            f"alertas={len(alertas)} | "
            f"total_scored={self._registry.total_scored} | "
            f"total_alerts={self._registry.total_alerts}"
        )