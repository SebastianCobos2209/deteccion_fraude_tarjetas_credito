"""
services/alert_evaluator.py
─────────────────────────────────────────────────────────────────
Evalúa si un fraud_score debe generar una alerta.
"""
from __future__ import annotations

from config.settings import FRAUD_THRESHOLD


class AlertEvaluator:

    def __init__(self, threshold: float = FRAUD_THRESHOLD) -> None:
        """
        Args:
            threshold: umbral de clasificación en [0, 1]
                       Scores >= threshold generan alerta
                       Default: FRAUD_THRESHOLD
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError(
                f"threshold debe estar en [0, 1], recibido: {threshold}"
            )
        self._threshold = threshold

    @property
    def threshold(self) -> float:
        return self._threshold

    def is_suspicious(self, fraud_score: float) -> bool:
        """
        Args:
            fraud_score: valor en [0, 1] devuelto por ModelRegistry.score()
                         Un valor de -1.0 indica modelo no listo → False

        Returns:
            True  → transacción sospechosa, debe generar alerta.
            False → transacción normal o modelo no listo.
        """
        if fraud_score < 0:
            return False
        return fraud_score >= self._threshold