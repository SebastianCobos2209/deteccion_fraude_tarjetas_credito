"""
infrastructure/contracts/vertexon_client.py
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List


class VertexonClient(ABC):
    @abstractmethod
    def get_customer(self) -> dict:
        """
        Returns:
            Dict con al menos: firstName, lastName, email, dateOfBirth.
            Retorna datos mock si el API no responde.
        """
        ...

    @abstractmethod
    def get_card_transactions(self) -> List[dict]:
        """
        Returns:
            Lista de dicts de transacciones. Mínimo un elemento
            (fallback mock) si el API no responde o lista vacía
        """
        ...

    @abstractmethod
    def get_card_rsa(self) -> dict:
        """
        Returns:
            Dict con al menos: expiryDate, cardCVV2Enc.
            Retorna datos mock si el API no responde.
        """
        ...