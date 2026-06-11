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
from datetime import datetime


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

    def _build_enriched(self, tx, features_eng, fraud_score, epoch_id) -> dict:
        return {
            # Identificadores
            "transaccionID":  tx.get("transaccionID"),
            "usuarioID":      tx.get("usuarioID"),
            "tarjetaID":      tx.get("tarjetaID"),
            # Variables numéricas
            "TransactionAmt": tx.get("TransactionAmt"),
            "TransactionDT":  tx.get("TransactionDT"),
            "card1":  tx.get("card1"),
            "card4":  tx.get("card4"),
            "card6":  tx.get("card6"),
            "ProductCD":      tx.get("ProductCD"),
            "P_emaildomain":  tx.get("P_emaildomain"),
            "addr1":  tx.get("addr1"),
            "addr2":  tx.get("addr2"),
            "DeviceType":     tx.get("DeviceType"),
            "DeviceInfo":     tx.get("DeviceInfo"),
            "C1":   tx.get("C1"),   "C13": tx.get("C13"),
            "C7":   tx.get("C7"),   "C14": tx.get("C14"),
            "D1":   tx.get("D1"),
            "V314": tx.get("V314"), "V201": tx.get("V201"),
            "V243": tx.get("V243"), "V257": tx.get("V257"),
            "V242": tx.get("V242"), "V45":  tx.get("V45"),
            "V246": tx.get("V246"), "V200": tx.get("V200"),
            "V258": tx.get("V258"),
            # Features engineered
            "zscore_amt":   features_eng.get("zscore_amt"),
            "velocity":     features_eng.get("velocity"),
            "amt_distance": features_eng.get("amt_distance"),
            # Inferencia
            "fraud_score":   round(fraud_score, 4) if fraud_score >= 0 else None,
            "is_suspicious": self._evaluator.is_suspicious(fraud_score) if fraud_score >= 0 else False,
            "is_warmed_up":  self._registry.is_warmed_up,
            "model_ready":   self._registry.is_warmed_up,   # ← mantener por compatibilidad
            "processed_at":  datetime.utcnow(),
            "isFraud":       tx.get("isFraud"),              # ← label del producer
    }

    def _build_alert(self, tx, features_eng, fraud_score) -> dict:
        return {
            "transaccionID":  tx.get("transaccionID"),
            "usuarioID":      tx.get("usuarioID"),
            "tarjetaID":      tx.get("tarjetaID"),
            "TransactionAmt": tx.get("TransactionAmt"),
            "fraud_score":    round(fraud_score, 4),
            "zscore_amt":     features_eng.get("zscore_amt"),
            "velocity":       features_eng.get("velocity"),
            "isFraud_label":  tx.get("isFraud"),
            "alerted_at":     datetime.utcnow(),   # ← date object
            # Campos de gestión operativa (schema fraud_alerts)
            "status":         "flagged",           # ← estado inicial siempre flagged
            "reviewed_by":    None,
            "reviewed_at":    None,
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