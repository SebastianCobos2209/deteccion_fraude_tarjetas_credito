"""
models_ai/sigmoid_normalizer.py
"""
from __future__ import annotations

import math

from models_ai.contracts.score_normalizer import ScoreNormalizer


class SigmoidScoreNormalizer(ScoreNormalizer):

    def __init__(self, k: float = 5.0) -> None:
        """
        Args:
            k: pendiente de la sigmoide.
               k=5 es el valor calibrado experimentalmente para
               datos IEEE-CIS con contamination=0.035.
        """
        self._k = k

    def normalize(self, score_samples: float, offset: float) -> float:
        """
        Args:
            score_samples: output de model.score_samples(X)[0].
                           Log-density: más negativo = más anómalo.
            offset:        model.offset_ del TrainedModel.
                           Threshold interno calibrado con contamination.

        Returns:
            fraud_score en [0.0, 1.0].
        """
        if abs(offset) < 1e-10:
            return 0.5

        exponent    = self._k * (score_samples - offset) / abs(offset)
        fraud_score = 1.0 / (1.0 + math.exp(exponent))
        return max(0.0, min(1.0, fraud_score))