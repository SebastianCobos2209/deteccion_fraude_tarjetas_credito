"""
infrastructure/kafka_alert_publisher.py
"""
from __future__ import annotations

import json
from typing import List

from pyspark.sql import SparkSession

from config.settings import KAFKA_BROKER, TOPIC_ALERTS
from infrastructure.contracts.alert_publisher import AlertPublisher
from utils.logger import get_logger

logger = get_logger("KafkaAlertPublisher")


class KafkaAlertPublisher(AlertPublisher):

    def __init__(
        self,
        broker: str = KAFKA_BROKER,
        topic:  str = TOPIC_ALERTS,
    ) -> None:
        self._broker = broker
        self._topic  = topic

    def publish(self, spark: SparkSession, alertas: List[dict]) -> None:
        if not alertas:
            return
        try:
            rdd = spark.sparkContext.parallelize(
                [json.dumps(a, ensure_ascii=False) for a in alertas]
            )
            df = spark.createDataFrame(rdd.map(lambda x: (x,)), ["value"])
            (df.write
               .format("kafka")
               .option("kafka.bootstrap.servers", self._broker)
               .option("topic", self._topic)
               .save())
            logger.info(
                f"{len(alertas)} alertas publicadas en {self._topic}"
            )
        except Exception as e:
            logger.error(f"Error publicando en {self._topic}: {e}")