"""
utils/timers.py
Utilidades para medir tiempos y gestionar intervalos.
"""
import time


def segundos_desde(timestamp: float) -> float:
    return time.time() - timestamp


def ha_pasado(timestamp: float, segundos: int) -> bool:
    return segundos_desde(timestamp) >= segundos
