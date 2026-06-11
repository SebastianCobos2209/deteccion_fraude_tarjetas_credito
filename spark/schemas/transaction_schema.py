"""
schemas/transaction_schema.py
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, LongType,
)

TX_SCHEMA = StructType([
    # ── Identificadores (Vertexon) ────────────────────────
    StructField("transaccionID",          StringType(),  True),
    StructField("fecha_hora_transaccion", StringType(),  True),
    StructField("usuarioID",              StringType(),  True),
    StructField("terminalID",             StringType(),  True),
    StructField("tarjetaID",              StringType(),  True),
    # ── Variables numéricas IEEE-CIS ──────────────────────
    StructField("TransactionAmt",         DoubleType(),  True),
    StructField("TransactionDT",          LongType(),    True),
    StructField("card1",                  IntegerType(), True),
    StructField("addr1",                  DoubleType(),  True),
    StructField("addr2",                  DoubleType(),  True),
    StructField("C1",                     IntegerType(), True),
    StructField("C13",                    IntegerType(), True),
    StructField("D1",                     IntegerType(), True),
    StructField("V314",                   DoubleType(),  True),
    StructField("V201",                   DoubleType(),  True),
    StructField("V243",                   DoubleType(),  True),
    StructField("V257",                   DoubleType(),  True),
    StructField("C7",                     IntegerType(), True),
    StructField("V242",                   DoubleType(),  True),
    StructField("V45",                    DoubleType(),  True),
    StructField("V246",                   DoubleType(),  True),
    StructField("V200",                   DoubleType(),  True),
    StructField("V258",                   DoubleType(),  True),
    StructField("C14",                    IntegerType(), True),
    # ── Variables categóricas IEEE-CIS ────────────────────
    StructField("ProductCD",              StringType(),  True),
    StructField("card4",                  StringType(),  True),
    StructField("card6",                  StringType(),  True),
    StructField("P_emaildomain",          StringType(),  True),
    StructField("DeviceType",             StringType(),  True),
    StructField("DeviceInfo",             StringType(),  True),
    # ── Etiqueta (PoC) ────────────────────────────────────
    StructField("isFraud",                IntegerType(), True),
])
