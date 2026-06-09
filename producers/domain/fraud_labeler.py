"""
domain/fraud_labeler.py
"""
from __future__ import annotations

import numpy as np


class FraudLabeler:
    _PROB_BOOST_AMT:  float = 0.15   
    _PROB_BOOST_V314: float = 0.10   
    _PROB_MAX:        float = 0.85

    def __init__(self, params) -> None:
        """
        Args:
            params: IEEEParameters con contamination_rate y
                    los estadísticos de TransactionAmt y V314
                    Se acepta duck typing para facilitar mocks en tests
        """
        self._params = params

    def label(self, amt: float, v314: float) -> int:
        """
        Args:
            amt:  valor de TransactionAmt de la transacción
            v314: valor de V314 de la transacción

        Returns:
            1 si la transacción es etiquetada como fraude
            0 si es legítima
        """
        prob = self._params.contamination_rate

        amt_mean, amt_std, _, _ = self._params.numeric["TransactionAmt"]
        if amt > amt_mean + 2 * amt_std:
            prob += self._PROB_BOOST_AMT

        v314_mean, v314_std, _, _ = self._params.numeric["V314"]
        if v314 > v314_mean + v314_std:
            prob += self._PROB_BOOST_V314

        prob = min(prob, self._PROB_MAX)
        return int(np.random.binomial(1, prob))

    def fraud_inflation_factor(self) -> float:
        """
        Returns:
            Factor aleatorio en [2.5, 8.0]
        """
        return float(np.random.uniform(2.5, 8.0))

    def apply_fraud_inflation(
        self,
        amt:  float,
        factor: float,
    ) -> float:
        """
        Args:
            amt:    monto original generado por Cholesky
            factor: multiplicador de inflación

        Returns:
            Monto inflado y clipeado al rango válido
        """
        _, _, amt_min, amt_max = self._params.numeric["TransactionAmt"]
        inflated = amt * factor
        return float(np.clip(inflated, amt_min, amt_max))