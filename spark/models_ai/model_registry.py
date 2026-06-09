"""
models_ai/model_registry.py
"""
from __future__ import annotations

import threading
import time
from typing import Optional

import numpy as np

from models_ai.contracts.model_persistence import ModelPersistence
from models_ai.contracts.model_trainer     import ModelTrainer, TrainedModel
from models_ai.contracts.score_normalizer  import ScoreNormalizer
from utils.logger import get_logger

logger = get_logger("ModelRegistry")


class ModelRegistry:

    def __init__(
        self,
        trainer:     ModelTrainer,
        persistence: ModelPersistence,
        normalizer:  ScoreNormalizer,
        fraud_threshold: float = 0.5,
    ) -> None:
        self._trainer         = trainer
        self._persistence     = persistence
        self._normalizer      = normalizer
        self._fraud_threshold = fraud_threshold

        self._lock           = threading.Lock()
        self._trained_model: Optional[TrainedModel] = None
        self._is_training    = False
        self._from_cache     = False
        self._last_trained   = time.time()
        self._total_scored   = 0
        self._total_alerts   = 0

        self._load_from_disk()

    # ── Propiedades públicas ──────────────────────────────

    @property
    def is_ready(self) -> bool:
        """True si hay un modelo activo (desde disco o entrenado)."""
        with self._lock:
            return self._trained_model is not None

    @property
    def is_warmed_up(self) -> bool:
        """
        True solo cuando el modelo fue entrenado con datos frescos
        de la sesión actual
        """
        with self._lock:
            return (
                self._trained_model is not None
                and not self._from_cache
            )

    @property
    def is_training(self) -> bool:
        with self._lock:
            return self._is_training

    @property
    def last_trained(self) -> float:
        with self._lock:
            return self._last_trained

    @property
    def total_scored(self) -> int:
        with self._lock:
            return self._total_scored

    @property
    def total_alerts(self) -> int:
        with self._lock:
            return self._total_alerts

    # ── Carga desde disco ─────────────────────────────────

    def _load_from_disk(self) -> None:
        """
        Intenta cargar el modelo previo desde disco al arrancar.
        Si existe, lo activa como modelo de caché (_from_cache=True).
        El scoring estará activo pero is_warmed_up=False hasta que
        WarmupOrchestrator complete el entrenamiento con datos frescos.
        """
        trained_model = self._persistence.load()
        if trained_model is not None:
            with self._lock:
                self._trained_model = trained_model
                self._from_cache    = True
            logger.warning(
                "Modelo cargado desde disco. "
                "is_warmed_up=False hasta completar warm-up con datos frescos."
            )

    # ── Entrenamiento asíncrono ───────────────────────────

    def train_async(self, X: np.ndarray) -> None:
        """
        Lanza el entrenamiento en un thread de background
        para no bloquear el stream principal de Spark.

        Si ya hay un entrenamiento en curso, no lanza otro.
        """
        with self._lock:
            if self._is_training:
                return
            self._is_training = True

        threading.Thread(
            target=self._training_cycle,
            args=(X,),
            daemon=True,
        ).start()

    def _training_cycle(self, X: np.ndarray) -> None:
        """Ciclo completo: entrenar → guardar → activar."""
        try:
            trained_model = self._trainer.train(X)
            self._persistence.save(trained_model)

            with self._lock:
                self._trained_model = trained_model
                self._from_cache    = False        # datos frescos
                self._is_training   = False
                self._last_trained  = time.time()

            logger.info(
                f"Modelo actualizado. "
                f"is_warmed_up=True | "
                f"n_samples={trained_model.n_samples} | "
                f"offset={trained_model.offset:.4f}"
            )
        except Exception as e:
            logger.error(f"Error en entrenamiento: {e}")
            with self._lock:
                self._is_training = False

    # ── Scoring ───────────────────────────────────────────

    def score(self, features: np.ndarray) -> float:
        """
        Calcula el fraud_score de una transacción.

        Returns:
            fraud_score en [0.0, 1.0]
            -1.0 si el modelo aún no está listo (warm-up en curso)
        """
        with self._lock:
            if self._trained_model is None:
                return -1.0
            try:
                X_scaled      = self._trained_model.scaler.transform(
                                    features.reshape(1, -1)
                                )
                score_samples = float(
                    self._trained_model.estimator.score_samples(X_scaled)[0]
                )
                fraud_score   = self._normalizer.normalize(
                    score_samples,
                    self._trained_model.offset,
                )

                self._total_scored += 1
                if fraud_score >= self._fraud_threshold:
                    self._total_alerts += 1

                return fraud_score

            except Exception as e:
                logger.error(f"Error en scoring: {e}")
                return -1.0