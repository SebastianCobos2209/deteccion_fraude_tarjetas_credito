"""
builders/tarjeta_builder.py
"""
from __future__ import annotations
import random
import uuid
from cryptography.fernet import Fernet
from config.settings                 import ProducerConfig
from domain.entities                 import Tarjeta
from statistics.correlated_generator import CorrelatedNumericGenerator
from statistics.categorical_sampler  import CategoricalSampler


class TarjetaBuilder:
    def __init__(
        self,
        generator: CorrelatedNumericGenerator,
        sampler:   CategoricalSampler,
        config:    ProducerConfig,
    ) -> None:
        self._generator = generator
        self._sampler   = sampler
        self._config    = config
        self._cipher    = Fernet(config.encryption_key)

    def build(self, card_rsa: dict, usuario_id: str) -> Tarjeta:
        """
        Args:
            card_rsa:   dict con datos de la tarjeta del mock Vertexon
                        Campos usados: expiryDate, cardCVV2Enc
            usuario_id: ID del usuario propietario de la tarjeta
        Returns:
            Tarjeta inmutable lista para publicar en Kafka
        """
        num = self._generator.generate()

        return Tarjeta(
            tarjetaID          = str(uuid.uuid4()),
            usuarioID          = usuario_id,
            card_number_masked = (
                f"**** **** **** {self._config.mock_card_number[-4:]}"
            ),
            fecha_exp_tarjeta  = self._parsear_expiry(
                card_rsa.get("expiryDate", "2612")
            ),
            cvv                = self._cifrar_cvv(
                card_rsa.get("cardCVV2Enc", "")
            ),
            card1              = int(num["card1"]),
            card4              = self._sampler.sample("card4"),
            card6              = self._sampler.sample("card6"),
            ProductCD          = self._sampler.sample("ProductCD"),
        )

    @staticmethod
    def _parsear_expiry(expiry: str) -> str:
        try:
            return f"{expiry[2:]}/20{expiry[:2]}"
        except (IndexError, TypeError):
            return "12/2026"

    def _cifrar_cvv(self, cvv_vertexon: str) -> str:
        """
        Args:
            cvv_vertexon: valor de cardCVV2Enc del response de Vertexon
        Returns:
            CVV cifrado como string
        """
        if len(cvv_vertexon) >= 20:
            return cvv_vertexon
        return self._cipher.encrypt(
            str(random.randint(100, 999)).encode()
        ).decode()