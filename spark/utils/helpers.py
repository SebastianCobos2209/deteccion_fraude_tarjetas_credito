"""
utils/helpers.py
Funciones auxiliares de uso general.
"""
from datetime import datetime


def ahora_iso() -> str:
    return datetime.now().isoformat()


def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
