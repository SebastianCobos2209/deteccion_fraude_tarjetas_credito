"""
producer_factory.py
"""
from __future__ import annotations

import argparse

from config.settings              import ProducerConfig
from domain.fraud_labeler         import FraudLabeler
from statistics.ieee_parameters   import IEEEParameters
from statistics.correlated_generator import CorrelatedNumericGenerator
from statistics.categorical_sampler  import CategoricalSampler
from builders.usuario_builder     import UsuarioBuilder
from builders.tarjeta_builder     import TarjetaBuilder
from builders.transaccion_builder import TransaccionBuilder
from infrastructure.contracts.vertexon_client import VertexonClient
from infrastructure.vertexon_http_client  import VertexonHttpClient
from infrastructure.mock_vertexon_client  import MockVertexonClient
from infrastructure.kafka_event_publisher import KafkaEventPublisher
from services.producer_service    import ProducerService
from utils.logger import get_logger

logger = get_logger("ProducerFactory")


class ProducerFactory:

    @staticmethod
    def build(args: argparse.Namespace | None = None) -> ProducerService:
        """
        Args:
            args: namespace de argparse con intervalo y variacion
                  Si es None se usan los valores de variables de entorno
                  o los defaults de ProducerConfig

        Returns:
            ProducerService listo para llamar a run()
        """
        logger.info("Construyendo grafo de dependencias del producer...")

        config = (
            ProducerConfig.from_args(args)
            if args is not None
            else ProducerConfig()
        )

        params = IEEEParameters()
        generator = CorrelatedNumericGenerator(params)
        sampler   = CategoricalSampler(params)
        labeler = FraudLabeler(params)
        usr_builder = UsuarioBuilder(generator, sampler, params, config)
        tar_builder = TarjetaBuilder(generator, sampler, config)
        tx_builder  = TransaccionBuilder(generator, labeler, config)
        vertexon: VertexonClient = ProducerFactory._build_vertexon_client(config)
        publisher = KafkaEventPublisher(config)
        service = ProducerService(
            vertexon    = vertexon,
            publisher   = publisher,
            usr_builder = usr_builder,
            tar_builder = tar_builder,
            tx_builder  = tx_builder,
            params      = params,
            config      = config,
        )

        logger.info(
            f"Producer construido | "
            f"broker={config.kafka_broker} | "
            f"intervalo={config.intervalo}s | "
            f"variacion={config.variacion} usuarios/ciclo"
        )
        return service

    @staticmethod
    def _build_vertexon_client(config: ProducerConfig) -> VertexonClient:
        client = VertexonHttpClient(config)
        try:
            client.get_customer()
            logger.info("VertexonHttpClient activo — mock SwaggerHub disponible.")
            return client
        except Exception:
            logger.warning(
                "SwaggerHub no disponible. "
                "Usando MockVertexonClient con datos fijos."
            )
            return MockVertexonClient()