"""
infrastructure/spark_session_factory.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import SPARK_MASTER, SPARK_APP_NAME
from utils.logger import get_logger

logger = get_logger("SparkSessionFactory")


class SparkSessionFactory:
    @staticmethod
    def create(
        master:              str = SPARK_MASTER,
        app_name:            str = SPARK_APP_NAME,
        shuffle_partitions:  int = 4,
        log_level:           str = "WARN",
    ) -> SparkSession:
        """
        Args:
            master:             URL del Spark master
            app_name:           nombre de la aplicación en la UI de Spark
            shuffle_partitions: número de particiones para operaciones shuffle
                                4 es suficiente para este pipeline de streaming
            log_level:          nivel de log de Spark
        Returns:
            SparkSession configurada y lista para usar
        """
        logger.info(f"Iniciando SparkSession → {master}")

        spark = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.shuffle.partitions",             str(shuffle_partitions))
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(log_level)

        logger.info(
            f"SparkSession activa | "
            f"app={app_name} | "
            f"master={master} | "
            f"shuffle_partitions={shuffle_partitions}"
        )
        return spark