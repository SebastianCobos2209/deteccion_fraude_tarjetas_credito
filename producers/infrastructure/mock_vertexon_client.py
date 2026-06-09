"""
infrastructure/mock_vertexon_client.py
"""
from __future__ import annotations

from typing import List

from infrastructure.contracts.vertexon_client import VertexonClient


class MockVertexonClient(VertexonClient):
    _CUSTOMER: dict = {
        "firstName":   "James",
        "lastName":    "Bond",
        "email":       "jhon.doe@example.com",
        "dateOfBirth": "1999-01-02",
    }

    _TRANSACCION: dict = {
        "effectiveDate":     "2021-05-04",
        "postingTime":       "17:25:16",
        "cardToken":         "15555000000000874",
        "accountNbr":        "00000000001",
        "product":           "MasterCard Elite",
        "transactionId":     "123e4567-e89b-12d3-a456-426655440000",
        "transactionAmount": {"amount": "1050.25", "currencyCode": "840"},
    }

    _CARD_RSA: dict = {
        "expiryDate":  "2612",
        "cardCVV2Enc": "P/ndVwu/6XEBPoDmvT/...",
    }

    def get_customer(self) -> dict:
        return self._CUSTOMER

    def get_card_transactions(self) -> List[dict]:
        return [self._TRANSACCION]

    def get_card_rsa(self) -> dict:
        return self._CARD_RSA