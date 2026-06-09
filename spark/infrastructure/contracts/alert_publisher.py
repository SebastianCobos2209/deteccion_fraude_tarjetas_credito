"""
infrastructure/contracts/alert_publisher.py
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from pyspark.sql import SparkSession


class AlertPublisher(ABC):

    @abstractmethod
    def publish(self, spark: SparkSession, alertas: List[dict]) -> None:
        """
        Args:
            spark:   SparkSession activa del pipeline
            alertas: lista de dicts con los campos de la alerta
                     (transaccionID, fraud_score, alerted_at, etc.)
        """
        ...