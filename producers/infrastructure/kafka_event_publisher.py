"""
infrastructure/kafka_event_publisher.py
"""
from __future__ import annotations

import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import ProducerConfig
from infrastructure.contracts.event_publisher import EventPublisher
from utils.logger import get_logger

logger = get_logger("KafkaEventPublisher")


class KafkaEventPublisher(EventPublisher):
    def __init__(self, config: ProducerConfig) -> None:
        self._config   = config
        self._producer = self._crear_producer()

    def publish(self, topic: str, key: str, payload: dict) -> None:
        """
        Args:
            topic:   nombre del topic Kafka destino
            key:     clave de particionado
            payload: dict que se serializa a JSON UTF-8
        """
        try:
            self._producer.send(
                topic,
                key=key,
                value=payload,
            ).get(timeout=self._config.kafka_timeout)
        except KafkaError as e:
            logger.error(f"Error publicando en '{topic}': {e}")
        except Exception as e:
            logger.error(f"Error inesperado publicando en '{topic}': {e}")

    def flush(self) -> None:
        try:
            self._producer.flush()
        except Exception as e:
            logger.error(f"Error en flush: {e}")

    def close(self) -> None:
        try:
            self._producer.close()
        except Exception as e:
            logger.error(f"Error cerrando producer: {e}")

    def _crear_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers = self._config.kafka_broker,
            value_serializer  = lambda v: json.dumps(
                v, ensure_ascii=False
            ).encode("utf-8"),
            key_serializer    = lambda k: (
                k.encode("utf-8") if k else None
            ),
            retries           = self._config.kafka_retries,
            acks              = self._config.kafka_acks,
        )