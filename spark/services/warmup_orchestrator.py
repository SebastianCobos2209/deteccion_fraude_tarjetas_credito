"""
services/warmup_orchestrator.py
"""
from __future__ import annotations

from typing import List

from models_ai.buffers.warmup_buffer import WarmupBuffer
from models_ai.model_registry        import ModelRegistry
from utils.logger import get_logger

logger = get_logger("WarmupOrchestrator")


class WarmupOrchestrator:
    def __init__(
        self,
        warmup_buffer: WarmupBuffer,
        registry:      ModelRegistry,
    ) -> None:
        self._buffer   = warmup_buffer
        self._registry = registry
        self._launched = False 

    @property
    def is_complete(self) -> bool:
        return self._launched

    @property
    def progress(self) -> str:
        count = min(self._buffer.count, self._buffer.warmup_size)
        return f"{count}/{self._buffer.warmup_size}"

    def notify(self, features: List[float]) -> None:
        """
        Args:
            features: vector de features de una transacción.
                      Debe tener la misma longitud que FEATURE_COLS
        """
        if self._launched:
            return

        self._buffer.add(features)

        if self._buffer.is_ready and not self._launched:
            self._launched = True
            X = self._buffer.get_data()
            logger.info(
                f"Warm-up completado con {len(X)} muestras. "
                f"Iniciando entrenamiento inicial en background..."
            )
            self._registry.train_async(X)
            self._buffer.reset()