"""
builders/transaccion_builder.py
"""
from __future__ import annotations
import uuid
from datetime import datetime
from config.settings                 import ProducerConfig
from domain.entities                 import Transaccion, Usuario, Tarjeta
from domain.fraud_labeler            import FraudLabeler
from statistics.correlated_generator import CorrelatedNumericGenerator


class TransaccionBuilder:
    def __init__(
        self,
        generator: CorrelatedNumericGenerator,
        labeler:   FraudLabeler,
        config:    ProducerConfig,
    ) -> None:
        self._generator = generator
        self._labeler   = labeler
        self._config    = config

    def build(
        self,
        tx_base: dict,
        usuario: Usuario,
        tarjeta: Tarjeta,
    ) -> Transaccion:
        """
        Args:
            tx_base: dict con datos base de la transacción de Vertexon
                     Campo usado: cardToken (para terminalID)
            usuario: entidad Usuario propietario de la tarjeta
            tarjeta: entidad Tarjeta usada en la transacción
        Returns:
            Transaccion inmutable lista para publicar en Kafka.
            Su to_dict() es compatible con TX_SCHEMA de Spark.
        """
        num    = self._generator.generate()
        fraude = self._labeler.label(num["TransactionAmt"], num["V314"])
        if fraude:
            factor = self._labeler.fraud_inflation_factor()
            num["TransactionAmt"] = self._labeler.apply_fraud_inflation(
                num["TransactionAmt"], factor
            )

        return Transaccion(
            transaccionID          = str(uuid.uuid4()),
            fecha_hora_transaccion = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            usuarioID              = usuario.usuarioID,
            terminalID             = tx_base.get(
                "cardToken", self._config.mock_card_token
            ),
            tarjetaID              = tarjeta.tarjetaID,
            TransactionAmt         = num["TransactionAmt"],
            TransactionDT          = num["TransactionDT"],
            card1                  = int(num["card1"]),
            addr1                  = num["addr1"],
            addr2                  = num["addr2"],
            C1                     = int(num["C1"]),
            C13                    = int(num["C13"]),
            D1                     = int(num["D1"]),
            V314                   = num["V314"],
            V201                   = num["V201"],
            V243                   = num["V243"],
            V257                   = num["V257"],
            C7                     = int(num["C7"]),
            V242                   = num["V242"],
            V45                    = num["V45"],
            V246                   = num["V246"],
            V200                   = num["V200"],
            V258                   = num["V258"],
            C14                    = int(num["C14"]),
            ProductCD              = tarjeta.ProductCD,
            card4                  = tarjeta.card4,
            card6                  = tarjeta.card6,
            P_emaildomain          = usuario.P_emaildomain,
            DeviceType             = usuario.DeviceType,
            DeviceInfo             = usuario.DeviceInfo,
            isFraud                = fraude,
        )