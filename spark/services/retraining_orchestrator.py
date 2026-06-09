"""
services/retraining_orchestrator.py
"""
from __future__ import annotations

from models_ai.buffers.sliding_window_buffer import SlidingWindowBuffer
from models_ai.model_registry                import ModelRegistry
from config.settings                          import RETRAIN_INTERVAL
from utils.timers                             import ha_pasado
from utils.logger                             import get_logger

logger = get_logger("RetrainingOrchestrator")


class RetrainingOrchestrator:
    def __init__(
        self,
        window_buffer:    SlidingWindowBuffer,
        registry:         ModelRegistry,
        retrain_interval: int = RETRAIN_INTERVAL,
    ) -> None:
        self._window   = window_buffer
        self._registry = registry
        self._interval = retrain_interval

    def notify(self, features: list) -> None:
        """
        Args:
            features: vector de features de una transacción.
        """
        self._window.add(features)

    def maybe_retrain(self) -> None:
        if not self._should_retrain():
            return

        X = self._window.get_data()
        logger.info(
            f"Iniciando re-entrenamiento periódico "
            f"con ventana de {len(X)} transacciones..."
        )
        self._registry.train_async(X)

    def _should_retrain(self) -> bool:
        return (
            self._registry.is_warmed_up
            and not self._registry.is_training
            and ha_pasado(self._registry.last_trained, self._interval)
            and self._window.has_enough_data()
        )