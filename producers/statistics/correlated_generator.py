"""
statistics/correlated_generator.py
"""
from __future__ import annotations

from typing import Dict, Set

import numpy as np

from statistics.ieee_parameters import IEEEParameters


_ROUND_3: Set[str] = {
    "TransactionAmt", "V314", "V201", "V243", "V257",
    "V242", "V45", "V246", "V200", "V258",
}

_INTEGER: Set[str] = {"TransactionDT"}


class CorrelatedNumericGenerator:
    """
    Uso:
        params    = IEEEParameters()
        generator = CorrelatedNumericGenerator(params)
        numericas = generator.generate()   # dict con 19 variables
    """

    def __init__(self, params: IEEEParameters) -> None:
        self._params = params

    def generate(self) -> Dict[str, float]:
        """
        Returns:
            Dict con una muestra por variable, redondeada según
            la naturaleza de cada variable (float3, int, float2)
        """
        z_corr = self._params.cholesky @ np.random.standard_normal(
            len(self._params.numeric_vars)
        )

        resultado: Dict[str, float] = {}
        for i, nombre in enumerate(self._params.numeric_vars):
            mean, std, vmin, vmax = self._params.numeric[nombre]
            valor = float(np.clip(mean + std * z_corr[i], vmin, vmax))
            resultado[nombre] = self._redondear(nombre, valor)

        return resultado

    @staticmethod
    def _redondear(nombre: str, valor: float):
        if nombre in _ROUND_3:
            return round(valor, 3)
        if nombre in _INTEGER:
            return int(valor)
        return round(valor, 2)