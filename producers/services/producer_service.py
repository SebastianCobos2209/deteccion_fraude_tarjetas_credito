"""
services/producer_service.py
"""
from __future__ import annotations

import time
from datetime import datetime

from builders.usuario_builder     import UsuarioBuilder
from builders.tarjeta_builder     import TarjetaBuilder
from builders.transaccion_builder import TransaccionBuilder
from config.settings              import ProducerConfig
from infrastructure.contracts.vertexon_client import VertexonClient
from infrastructure.contracts.event_publisher import EventPublisher
from statistics.ieee_parameters   import IEEEParameters
from utils.logger import get_logger

logger = get_logger("ProducerService")


class ProducerService:
    def __init__(
        self,
        vertexon:     VertexonClient,
        publisher:    EventPublisher,
        usr_builder:  UsuarioBuilder,
        tar_builder:  TarjetaBuilder,
        tx_builder:   TransaccionBuilder,
        params:       IEEEParameters,
        config:       ProducerConfig,
    ) -> None:
        self._vertexon    = vertexon
        self._publisher   = publisher
        self._usr_builder = usr_builder
        self._tar_builder = tar_builder
        self._tx_builder  = tx_builder
        self._params      = params
        self._config      = config
        self._total_tx:    int = 0
        self._total_fraud: int = 0
        self._ciclos:      int = 0


    def run(self) -> None:
        self._print_banner()

        try:
            while True:
                self._ciclos += 1
                fraudes_ciclo = self._run_cycle(self._ciclos)
                self._print_cycle_log(fraudes_ciclo)
                time.sleep(self._config.intervalo)

        except KeyboardInterrupt:
            print("\n  Detenido por el usuario.")
        finally:
            self._publisher.flush()
            self._publisher.close()
            self._print_summary()


    def _run_cycle(self, ciclo: int) -> int:
        """
        Args:
            ciclo: número de ciclo actual.

        Returns:
            Número de fraudes generados en este ciclo.
        """
        customer = self._vertexon.get_customer()
        txs      = self._vertexon.get_card_transactions()
        card_rsa = self._vertexon.get_card_rsa()
        tx_base  = txs[0] if txs else {}

        fraudes_ciclo = 0

        for i in range(self._config.variacion):
            iteracion = (ciclo - 1) * self._config.variacion + i

            usuario  = self._usr_builder.build(customer, iteracion)
            tarjeta  = self._tar_builder.build(card_rsa, usuario.usuarioID)
            tx       = self._tx_builder.build(tx_base, usuario, tarjeta)
            self._publish_all(usuario, tarjeta, tx)
            self._total_tx    += 1
            self._total_fraud += tx.isFraud
            fraudes_ciclo     += tx.isFraud

        return fraudes_ciclo

    def _publish_all(self, usuario, tarjeta, tx) -> None:
        self._publisher.publish(
            topic   = self._config.topic_usuarios,
            key     = usuario.usuarioID,
            payload = usuario.to_dict(),
        )
        self._publisher.publish(
            topic   = self._config.topic_tarjetas,
            key     = tarjeta.tarjetaID,
            payload = tarjeta.to_dict(),
        )
        self._publisher.publish(
            topic   = self._config.topic_transacciones,
            key     = tx.transaccionID,
            payload = tx.to_dict(),
        )

    def _print_banner(self) -> None:
        """Banner inicial con la configuración de la sesión."""
        n_numeric = len(self._params.numeric_vars)
        tasa      = self._params.contamination_rate * 100
        sep       = "=" * 60
        print(f"\n{sep}")
        print(f"  Vertexon -> Kafka Producer  (IEEE-CIS v2 - {n_numeric} variables)")
        print(f"{sep}")
        print(f"  Kafka          : {self._config.kafka_broker}")
        print(f"  Variables num. : {n_numeric}")
        print(f"  Variables cat. : 6  (ProductCD, card4, card6, email, device)")
        print(f"  Contaminación  : {tasa:.2f}%  (IEEE-CIS real)")
        print(f"  Correlaciones  : Cholesky 19x19 activo")
        print(f"  Intervalo      : {self._config.intervalo}s  "
              f"| {self._config.variacion} usuarios/ciclo")
        print(f"{sep}")
        print("  Presiona Ctrl+C para detener.\n")

    def _print_cycle_log(self, fraudes_ciclo: int) -> None:
        tasa = (
            self._total_fraud / self._total_tx * 100
            if self._total_tx else 0.0
        )
        esperado = self._params.contamination_rate * 100
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"ciclo={self._ciclos:04d} | "
            f"txs={self._total_tx} | "
            f"fraudes={self._total_fraud} ({tasa:.1f}%) | "
            f"esperado={esperado:.1f}%"
        )

    def _print_summary(self) -> None:
        tasa     = self._total_fraud / self._total_tx * 100 if self._total_tx else 0.0
        esperado = self._params.contamination_rate * 100
        sep      = "=" * 60
        print(f"\n{sep}")
        print(f"  Ciclos    : {self._ciclos}")
        print(f"  Txs total : {self._total_tx}")
        print(f"  Fraudes   : {self._total_fraud} ({tasa:.2f}%)")
        print(f"  Esperado  : {esperado:.2f}%")
        print(f"{sep}")