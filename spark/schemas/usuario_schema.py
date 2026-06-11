"""
schemas/usuario_schema.py
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

USUARIO_SCHEMA = StructType([
    # ── Identificadores Vertexon ──────────────────────────
    StructField("usuarioID",                  StringType(), True),
    StructField("usuario",                    StringType(), True),
    StructField("vertexon_customer_number",   StringType(), True),
    StructField("fecha_nacimiento",           StringType(), True),
    # ── Contacto y dispositivo ────────────────────────────
    StructField("email",                      StringType(), True),
    StructField("P_emaildomain",              StringType(), True),
    StructField("DeviceType",                 StringType(), True),
    StructField("DeviceInfo",                 StringType(), True),
    # ── Geolocalización ───────────────────────────────────
    StructField("usuario_x",                  DoubleType(), True),
    StructField("usuario_y",                  DoubleType(), True),
    # ── Variables IEEE-CIS ────────────────────────────────
    StructField("addr1",                      DoubleType(), True),
    StructField("addr2",                      DoubleType(), True),
    # ── Estadísticas de gasto ─────────────────────────────
    StructField("promedio_de_gastos",         DoubleType(), True),
    StructField("varianza_de_gastos",         DoubleType(), True),
    StructField("promedio_de_gastos_por_dia", DoubleType(), True),
])