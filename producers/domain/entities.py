"""
domain/entities.py
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Usuario:
    usuarioID:                  str
    usuario:                    str
    vertexon_customer_number:   str
    fecha_nacimiento:           str
    email:                      str
    P_emaildomain:              str
    DeviceType:                 str
    DeviceInfo:                 str
    usuario_x:                  float
    usuario_y:                  float
    addr1:                      float
    addr2:                      float
    promedio_de_gastos:         float
    varianza_de_gastos:         float
    promedio_de_gastos_por_dia: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Tarjeta:
   
    tarjetaID:          str
    usuarioID:          str
    card_number_masked: str
    fecha_exp_tarjeta:  str
    cvv:                str
    card1:              int
    card4:              str    
    card6:              str    
    ProductCD:          str  

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Transaccion:
    transaccionID:          str
    fecha_hora_transaccion: str
    usuarioID:              str
    terminalID:             str
    tarjetaID:              str
    TransactionAmt: float
    TransactionDT:  int
    card1:          int
    addr1:          float
    addr2:          float
    C1:             int
    C13:            int
    D1:             int
    V314:           float
    V201:           float
    V243:           float
    V257:           float
    C7:             int
    V242:           float
    V45:            float
    V246:           float
    V200:           float
    V258:           float
    C14:            int
    ProductCD:      str
    card4:          str
    card6:          str
    P_emaildomain:  str
    DeviceType:     str
    DeviceInfo:     str
    isFraud:        int

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def es_fraude(self) -> bool:
        return self.isFraud == 1