"""
models_ai/buffers/warmup_buffer.py
"""
from __future__ import annotations

import threading
from typing import List

import numpy as np

from config.settings import WARMUP_SIZE
from utils.logger import get_logger

logger = get_logger("WarmupBuffer")


class WarmupBuffer:

    def __init__(self, warmup_size: int = WARMUP_SIZE) -> None:
        self._warmup_size = warmup_size
        self._data: List[List[float]] = []
        self._lock = threading.Lock()

    # ── Interfaz pública ──────────────────────────────────

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._data)

    @property
    def warmup_size(self) -> int:
        return self._warmup_size

    @property
    def is_ready(self) -> bool:
        return self.count >= self._warmup_size

    def add(self, features: List[float]) -> None:
        with self._lock:
            if len(self._data) >= self._warmup_size:
                return
            self._data.append(features)
            n = len(self._data)

        if n % 100 == 0:
            logger.info(f"Warm-up: {n}/{self._warmup_size} transacciones acumuladas")

    def get_data(self) -> np.ndarray:
        with self._lock:
            return np.array(self._data, dtype=np.float64)

    def reset(self) -> None:
        with self._lock:
            self._data.clear()
        logger.info("WarmupBuffer reseteado tras entrenamiento inicial.")