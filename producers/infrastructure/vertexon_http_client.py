"""
infrastructure/vertexon_http_client.py
"""
from __future__ import annotations

from typing import List

import requests

from config.settings import ProducerConfig
from infrastructure.contracts.vertexon_client import VertexonClient
from utils.logger import get_logger

logger = get_logger("VertexonHttpClient")

# ── Datos de fallback (del código original) ────────────────────
_FALLBACK_CUSTOMER = {
    "firstName":   "James",
    "lastName":    "Bond",
    "email":       "jhon.doe@example.com",
    "dateOfBirth": "1999-01-02",
}
_FALLBACK_TRANSACCION = {
    "effectiveDate":     "2021-05-04",
    "postingTime":       "17:25:16",
    "cardToken":         "15555000000000874",
    "accountNbr":        "00000000001",
    "product":           "MasterCard Elite",
    "transactionId":     "123e4567-e89b-12d3-a456-426655440000",
    "transactionAmount": {"amount": "1050.25", "currencyCode": "840"},
}
_FALLBACK_CARD_RSA = {
    "expiryDate":   "2612",
    "cardCVV2Enc":  "P/ndVwu/6XEBPoDmvT/...",
}


class VertexonHttpClient(VertexonClient):

    def __init__(self, config: ProducerConfig) -> None:
        self._config = config

    def get_customer(self) -> dict:
        url = (
            f"{self._config.mock_base_url}"
            f"/v1/customer/{self._config.mock_customer_nr}"
        )
        return self._get(url) or _FALLBACK_CUSTOMER

    def get_card_transactions(self) -> List[dict]:
        url = (
            f"{self._config.mock_base_url}"
            f"/v1/card/{self._config.mock_card_number}/transactions"
        )
        data = self._get(url, params={"lastNbrTransCard": 1})
        return (data or {}).get("transactions", [_FALLBACK_TRANSACCION])

    def get_card_rsa(self) -> dict:
        url = (
            f"{self._config.mock_base_url}"
            f"/v1/card/{self._config.mock_card_token}/cardRSAEncrypted"
        )
        return self._get(
            url,
            headers={"encodedKey": "MOCK_KEY", "includeCVV2": "true"},
        ) or _FALLBACK_CARD_RSA


    def _get(self, url: str, **kwargs) -> dict | None:
        try:
            response = requests.get(
                url,
                timeout=self._config.http_timeout,
                **kwargs,
            )
            if response.status_code == 200:
                return response.json()
            logger.warning(
                f"Vertexon respondió {response.status_code} en {url}"
            )
            return None
        except Exception as e:
            logger.warning(f"Error llamando a Vertexon [{url}]: {e}")
            return None