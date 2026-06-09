"""
models_ai/joblib_persistence.py
"""
from __future__ import annotations

import os
from typing import Optional

import joblib

from config.settings import MODEL_PATH, SCALER_PATH
from models_ai.contracts.model_persistence import ModelPersistence
from models_ai.contracts.model_trainer import TrainedModel
from utils.logger import get_logger

logger = get_logger("JobLibModelPersistence")


class JobLibModelPersistence(ModelPersistence):

    def __init__(
        self,
        model_path:  str = MODEL_PATH,
        scaler_path: str = SCALER_PATH,
    ) -> None:
        self._model_path  = model_path
        self._scaler_path = scaler_path

    def save(self, trained_model: TrainedModel) -> None:
        """
        Persiste el estimador y el scaler en disco.

        No lanza excepciones al caller — los errores se loguean
        internamente para no interrumpir el stream.
        """
        try:
            os.makedirs(os.path.dirname(self._model_path), exist_ok=True)
            joblib.dump(trained_model.estimator, self._model_path)
            joblib.dump(trained_model.scaler,    self._scaler_path)
            logger.info(
                f"Modelo guardado → {self._model_path} "
                f"| n_samples={trained_model.n_samples} "
                f"| offset={trained_model.offset:.4f}"
            )
        except Exception as e:
            logger.error(f"No se pudo guardar el modelo: {e}")

    def load(self) -> Optional[TrainedModel]:
        """
        Carga el modelo desde disco si existe.

        Returns:
            TrainedModel si ambos archivos existen y se cargan bien.
            None si es el primer arranque o si la carga falla.
        """
        if not (
            os.path.exists(self._model_path)
            and os.path.exists(self._scaler_path)
        ):
            logger.info("No hay modelo previo en disco. Iniciando warm-up.")
            return None

        try:
            import datetime
            mtime = os.path.getmtime(self._model_path)
            fecha = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

            estimator = joblib.load(self._model_path)
            scaler    = joblib.load(self._scaler_path)

            n_samples = getattr(estimator, "n_samples_fit_", -1)
            offset    = float(getattr(estimator, "offset_", 0.0))

            logger.warning(
                f"Modelo en disco cargado — guardado: {fecha} | "
                f"n_samples_fit_: {n_samples} | offset_: {offset:.4f}"
            )

            return TrainedModel(
                estimator = estimator,
                scaler    = scaler,
                n_samples = n_samples,
                offset    = offset,
            )
        except Exception as e:
            logger.error(f"No se pudo cargar el modelo previo: {e}")
            return None