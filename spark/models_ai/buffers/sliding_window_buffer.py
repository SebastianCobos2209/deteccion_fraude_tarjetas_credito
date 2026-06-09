"""
models_ai/buffers/sliding_window_buffer.py
─────────────────────────────────────────────────────────────────
Buffer de ventana deslizante: mantiene las N muestras más recientes
para alimentar el re-entrenamiento periódico del modelo.

"""
from __future__ import annotations

import threading
from collections import deque
from typing import List

import numpy as np

from config.settings import WARMUP_SIZE
from utils.logger import get_logger

logger = get_logger("SlidingWindowBuffer")

# Tamaño máximo de la ventana deslizante.
# 5000 muestras * ~19 features * 8 bytes ≈ 760 KB en memoria.
_WINDOW_MAXLEN = 5000


class SlidingWindowBuffer:

    def __init__(
        self,
        maxlen: int = _WINDOW_MAXLEN,
        min_samples: int = WARMUP_SIZE,
    ) -> None:
        self._window: deque = deque(maxlen=maxlen)
        self._min_samples = min_samples
        self._lock = threading.Lock()

    # ── Interfaz pública ──────────────────────────────────

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._window)

    @property
    def maxlen(self) -> int:
        return self._window.maxlen

    def has_enough_data(self) -> bool:
        return self.count >= self._min_samples

    def add(self, features: List[float]) -> None:
        with self._lock:
            self._window.append(features)

    def get_data(self) -> np.ndarray:
        with self._lock:
            return np.array(list(self._window), dtype=np.float64)