"""
statistics/categorical_sampler.py
"""
from __future__ import annotations

import numpy as np

from statistics.ieee_parameters import IEEEParameters


class CategoricalSampler:
    """
    Uso:
        params  = IEEEParameters()
        sampler = CategoricalSampler(params)
        card4   = sampler.sample("card4")        # "visa", "mastercard", ...
        device  = sampler.sample("DeviceType")   # "desktop", "mobile"
        email   = sampler.sample("P_emaildomain")
    """

    def __init__(self, params: IEEEParameters) -> None:
        self._params = params
        self._probs = self._precompute_probs()

    def _precompute_probs(self) -> dict:
        result = {}
        for var, freq_dict in self._params.categorical.items():
            keys  = list(freq_dict.keys())
            probs = np.array(list(freq_dict.values()), dtype=np.float64)
            probs = probs / probs.sum()
            result[var] = (keys, probs)
        return result

    def sample(self, variable: str) -> str:
        """
        Args:
            variable: nombre de la variable. Debe existir en
                      IEEEParameters.categorical
                      Valores válidos: "ProductCD", "card4", "card6",
                      "P_emaildomain", "DeviceType", "DeviceInfo"
        Returns:
            Valor categórico muestreado según frecuencias reales
        Raises:
            KeyError: si la variable no existe en IEEEParameters
        """
        if variable not in self._probs:
            raise KeyError(
                f"Variable categórica '{variable}' no encontrada. "
                f"Disponibles: {list(self._probs.keys())}"
            )
        keys, probs = self._probs[variable]
        return str(np.random.choice(keys, p=probs))