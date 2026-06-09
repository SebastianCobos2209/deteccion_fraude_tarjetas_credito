"""
infrastructure/contracts/event_publisher.py
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class EventPublisher(ABC):
    @abstractmethod
    def publish(self, topic: str, key: str, payload: dict) -> None:
        """
        Args:
            topic:   nombre del topic destino
                     Valores esperados: topic_usuarios, topic_tarjetas
                     topic_transacciones (de ProducerConfig)
            key:     clave del mensaje para particionado
                     Normalmente el ID de la entidad
            payload: dict con el contenido del mensaje
                     Se serializa a JSON UTF-8 antes de publicar
        """
        ...

    @abstractmethod
    def flush(self) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...