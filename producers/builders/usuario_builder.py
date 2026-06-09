"""
builders/usuario_builder.py
"""
from __future__ import annotations
import random
import uuid
from faker import Faker
from config.settings              import ProducerConfig
from domain.entities              import Usuario
from statistics.correlated_generator import CorrelatedNumericGenerator
from statistics.categorical_sampler  import CategoricalSampler
from statistics.ieee_parameters      import IEEEParameters


class UsuarioBuilder:
    def __init__(
        self,
        generator: CorrelatedNumericGenerator,
        sampler:   CategoricalSampler,
        params:    IEEEParameters,
        config:    ProducerConfig,
    ) -> None:
        self._generator = generator
        self._sampler   = sampler
        self._params    = params
        self._config    = config
        self._faker     = Faker(config.faker_locale)

    def build(self, customer: dict, iteracion: int) -> Usuario:
        """
        Args:
            customer:  dict con datos del cliente del mock Vertexon
                       Campos usados: lastName, dateOfBirth
            iteracion: número de iteración para el vertexon_customer_number
        Returns:
            Usuario inmutable listo para publicar en Kafka
        """
        num = self._generator.generate()

        _, amt_std, _, _ = self._params.numeric["TransactionAmt"]

        return Usuario(
            usuarioID                  = str(uuid.uuid4()),
            usuario                    = (
                f"{self._faker.first_name()} "
                f"{customer.get('lastName', 'Bond')}"
            ),
            vertexon_customer_number   = (
                f"{self._config.mock_customer_nr[:-3]}{iteracion:03d}"
            ),
            fecha_nacimiento           = customer.get("dateOfBirth", "1999-01-02"),
            email                      = self._sampler.sample("P_emaildomain"),
            P_emaildomain              = self._sampler.sample("P_emaildomain"),
            DeviceType                 = self._sampler.sample("DeviceType"),
            DeviceInfo                 = self._sampler.sample("DeviceInfo"),
            usuario_x                  = round(random.uniform(-180.0, 180.0), 6),
            usuario_y                  = round(random.uniform(-90.0,   90.0), 6),
            addr1                      = num["addr1"],
            addr2                      = num["addr2"],
            promedio_de_gastos         = round(num["TransactionAmt"], 3),
            varianza_de_gastos         = round(amt_std ** 2, 3),
            promedio_de_gastos_por_dia = round(
                num["TransactionAmt"] * random.uniform(0.3, 2.5), 3
            ),
        )