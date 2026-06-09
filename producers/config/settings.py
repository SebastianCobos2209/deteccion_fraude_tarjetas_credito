"""
config/settings.py
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from cryptography.fernet import Fernet


def _get_encryption_key() -> bytes:
    raw = os.getenv("ENCRYPTION_KEY")
    return raw.encode() if raw else Fernet.generate_key()


@dataclass(frozen=True)
class ProducerConfig:
    # ── Kafka ─────────────────────────────────────────────
    kafka_broker:        str   = field(
        default_factory=lambda: os.getenv("KAFKA_BROKER", "localhost:9092")
    )
    topic_usuarios:      str   = "vertexon.usuarios"
    topic_tarjetas:      str   = "vertexon.tarjetas"
    topic_transacciones: str   = "transactions.raw"

    # ── Vertexon mock ─────────────────────────────────────
    mock_base_url:       str   = (
        "https://virtserver.swaggerhub.com"
        "/Change_Financial/vertexon-CMS_open_api/0.1.7"
    )
    mock_customer_nr:    str   = "000000000001"
    mock_card_number:    str   = "5555000012348874"
    mock_card_token:     str   = "15555000000000874"
    http_timeout:        int   = 8     # segundos por request al mock

    encryption_key:      bytes = field(
        default_factory=_get_encryption_key
    )

    intervalo:           float = field(
        default_factory=lambda: float(os.getenv("INTERVALO", "2.0"))
    )
    variacion:           int   = field(
        default_factory=lambda: int(os.getenv("VARIACION", "5"))
    )

    kafka_retries:       int   = 5
    kafka_acks:          str   = "all"
    kafka_timeout:       int   = 10  

    faker_locale:        str   = "es_MX"

    @classmethod
    def from_args(cls, args) -> "ProducerConfig":
        """
            args   = parse_args()
            config = ProducerConfig.from_args(args)
        """
        return cls(
            intervalo = args.intervalo,
            variacion = args.variacion,
        )