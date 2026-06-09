"""
models_ai/isolation_forest_trainer.py
"""
from __future__ import annotations

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from config.settings import CONTAMINATION
from models_ai.contracts.model_trainer import ModelTrainer, TrainedModel
from utils.logger import get_logger

logger = get_logger("IsolationForestTrainer")


class IsolationForestTrainer(ModelTrainer):

    def __init__(
        self,
        n_estimators:  int   = 100,
        contamination: float = CONTAMINATION,
        max_samples:   str   = "auto",
        random_state:  int   = 42,
        n_jobs:        int   = -1,
    ) -> None:
        self._n_estimators  = n_estimators
        self._contamination = contamination
        self._max_samples   = max_samples
        self._random_state  = random_state
        self._n_jobs        = n_jobs

    def train(self, X: np.ndarray) -> TrainedModel:
        """
        Args:
            X: array de shape (n_samples, n_features) sin NaNs

        Returns:
            TrainedModel con estimador, scaler, n_samples y offset_

        Raises:
            ValueError: si X está vacío
        """
        if len(X) == 0:
            raise ValueError("No se puede entrenar con un array vacío.")

        logger.info(f"Entrenando IsolationForest con {len(X)} muestras...")

        scaler   = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        estimator = IsolationForest(
            n_estimators  = self._n_estimators,
            contamination = self._contamination,
            max_samples   = self._max_samples,
            random_state  = self._random_state,
            n_jobs        = self._n_jobs,
        )
        estimator.fit(X_scaled)

        logger.info(
            f"Entrenamiento completado. "
            f"offset_={estimator.offset_:.4f} | "
            f"contamination={self._contamination}"
        )

        return TrainedModel(
            estimator = estimator,
            scaler    = scaler,
            n_samples = len(X),
            offset    = float(estimator.offset_),
        )