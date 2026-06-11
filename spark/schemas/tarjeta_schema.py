"""
schemas/tarjeta_schema.py
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
)

TARJETA_SCHEMA = StructType([
    # ── Identificadores ───────────────────────────────────
    StructField("tarjetaID",          StringType(),  True),
    StructField("usuarioID",          StringType(),  True),
    # ── Datos de la tarjeta ───────────────────────────────
    StructField("card_number_masked", StringType(),  True),
    StructField("fecha_exp_tarjeta",  StringType(),  True),
    StructField("cvv",                StringType(),  True),
    # ── Variables IEEE-CIS ────────────────────────────────
    StructField("card1",              IntegerType(), True),
    StructField("card4",              StringType(),  True),
    StructField("card6",              StringType(),  True),
    StructField("ProductCD",          StringType(),  True),
])