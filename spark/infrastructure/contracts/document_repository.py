"""
infrastructure/contracts/document_repository.py
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List


class DocumentRepository(ABC):
    @abstractmethod
    def save(self, collection: str, documents: List[dict]) -> None:
        """
        Args:
            collection: nombre de la colección destino.
                        Valores esperados: COL_ENRICHED, COL_ALERTS
                        definidos en config/settings.py
            documents:  lista de dicts a insertar
        """
        ...