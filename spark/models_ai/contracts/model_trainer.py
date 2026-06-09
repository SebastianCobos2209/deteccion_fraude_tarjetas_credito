"""
models_ai/contracts/model_trainer.py
─────────────────────────────────────────────────────────────────
Contrato de entrenamiento de modelos de detección de anomalías.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class TrainedModel:
    estimator:  Any
    scaler:     Any
    n_samples:  int
    offset:     float


class ModelTrainer(ABC):

    @abstractmethod
    def train(self, X: np.ndarray) -> TrainedModel:
        ...