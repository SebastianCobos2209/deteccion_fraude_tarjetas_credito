"""
services/feature_engineering.py
"""
import numpy as np
from config.settings import FEATURE_COLS, CATEGORICAL_FRAUD_RATES,CATEGORICAL_DEFAULT_RATE, AMT_MEAN, AMT_STD
from utils.helpers import safe_float, safe_int


def extraer_features(tx: dict, user_profile: dict) -> np.ndarray:
    numericas = [safe_float(tx.get(col)) for col in FEATURE_COLS]

    categoricas = [
        CATEGORICAL_FRAUD_RATES[col].get(
            tx.get(col), CATEGORICAL_DEFAULT_RATE
        )
        for col in CATEGORICAL_FRAUD_RATES
    ]
    amt             = safe_float(tx.get("TransactionAmt"))
    avg_gasto       = safe_float(user_profile.get("promedio_de_gastos"), default=AMT_MEAN)
    var_gasto       = safe_float(user_profile.get("varianza_de_gastos"), default=AMT_STD**2)
    std_gasto       = max(var_gasto ** 0.5, 1.0) 

    ratio_vs_user   = amt / max(avg_gasto, 1.0)
    zscore_vs_user  = (amt - avg_gasto) / std_gasto

    comportamiento = [ratio_vs_user, zscore_vs_user]

    return np.array(numericas + categoricas + comportamiento, dtype=np.float64)


def zscore_amt(amt: float) -> float:
    return (amt - AMT_MEAN) / AMT_STD if AMT_STD > 0 else 0.0


def velocity(c1: int, d1: int) -> float:
    return float(c1) / float(max(d1, 1))


def amt_distance(amt: float) -> float:
    return abs(zscore_amt(amt))


def calcular_features_engineered(tx: dict) -> dict:
    amt = safe_float(tx.get("TransactionAmt"))
    c1  = safe_int(tx.get("C1"))
    d1  = safe_int(tx.get("D1"), default=1)
    return {
        "zscore_amt":   round(zscore_amt(amt),   4),
        "velocity":     round(velocity(c1, d1),  4),
        "amt_distance": round(amt_distance(amt), 4),
    }