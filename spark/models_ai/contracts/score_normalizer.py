"""
models_ai/contracts/score_normalizer.py
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class ScoreNormalizer(ABC):
    @abstractmethod
    def normalize(self, score_samples: float, offset: float) -> float:
        ...