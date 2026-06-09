"""
models_ai/contracts/model_persistence.py
─────────────────────────────────────────────────────────────────
Contrato de persistencia de modelos entrenados.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from models_ai.contracts.model_trainer import TrainedModel


class ModelPersistence(ABC):

    @abstractmethod
    def save(self, trained_model: TrainedModel) -> None:
        ...

    @abstractmethod
    def load(self) -> Optional[TrainedModel]:
        ...