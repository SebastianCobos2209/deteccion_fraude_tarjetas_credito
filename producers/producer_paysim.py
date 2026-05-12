"""
Vertexon Mock → Kafka Producer
───────────────────────────────
Consume el mock server de Vertexon (SwaggerHub) y publica
datos enriquecidos a tres topics de Kafka en tiempo real.

IMPORTANTE SOBRE EL MOCK:
  El mock siempre devuelve los mismos datos fijos (James Bond,
  tarjeta 5555000012348874, transacción de 1050.25).
  Este producer los toma como base y añade variación sintética
  para simular múltiples usuarios y transacciones reales.

Topics Kafka:
  • vertexon.usuarios
  • vertexon.tarjetas
  • vertexon.transacciones

Dependencias:
  pip install kafka-python requests faker numpy cryptography

Uso:
  python producer_paysim.py
  python producer_paysim.py --intervalo 3 --variacion 10
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import numpy as np
import requests
from cryptography.fernet import Fernet
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────

MOCK_BASE = "https://virtserver.swaggerhub.com/Change_Financial/vertexon-CMS_open_api/0.1.7"

KAFKA_BROKER        = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_USUARIOS      = "vertexon.usuarios"
TOPIC_TARJETAS      = "vertexon.tarjetas"
TOPIC_TRANSACCIONES = "transactions.raw"

# Encriptación del CVV con Fernet (AES-128)
# En producción: export ENCRYPTION_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
_raw_key       = os.getenv("ENCRYPTION_KEY")
ENCRYPTION_KEY = _raw_key.encode() if _raw_key else Fernet.generate_key()
cipher         = Fernet(ENCRYPTION_KEY)

fake = Faker("es_MX")

# Tiempo promedio por transacción — distribución log-normal
# Mediana real de autorización de pagos ≈ 1.5 s, sigma=0.6 (cola larga)
TIEMPO_PROMEDIO_TX = float(
    np.mean(np.random.lognormal(mean=np.log(1.5), sigma=0.6, size=5000))
)

# ─────────────────────────────────────────────────────────────
# DATOS REALES DEL MOCK (según la documentación de Vertexon)
# Estos son los únicos valores que el mock devuelve siempre.
# ─────────────────────────────────────────────────────────────

# GET /v1/customer/{customerNumber} → siempre devuelve:
MOCK_CUSTOMER = {
    "firstName":   "James",
    "middleName":  "Antony",
    "lastName":    "Bond",
    "email":       "jhon.doe@example.com",
    "dateOfBirth": "1999-01-02",
    "gender":      "M",
    "addresses": [{
        "city":    "New York",
        "state":   "NY",
        "country": "US",
    }]
}

# GET /v1/card/{cardNumber}/transactions → siempre devuelve:
MOCK_TRANSACCION = {
    "effectiveDate":   "2021-05-04",
    "postingTime":     "17:25:16",
    "cardToken":       "15555000000000874",
    "accountNbr":      "00000000001",
    "product":         "MasterCard Elite",
    "transDescription": "Transaction Sample",
    "transactionId":   "123e4567-e89b-12d3-a456-426655440000",
    "transactionAmount": {"amount": "1050.25", "currencyCode": "840"}
}

# GET /v1/card/{cardToken}/cardRSAEncrypted → siempre devuelve:
MOCK_CARD_RSA = {
    "cardNbrEnc":  "D8Qg/EgNDb/hEcF+fpw+...",   # número de tarjeta encriptado RSA
    "expiryDate":  "2612",                         # formato YYMM → diciembre 2026
    "cardCVV2Enc": "P/ndVwu/6XEBPoDmvT/..."       # CVV encriptado RSA por Vertexon
}

# Número de tarjeta y card token del mock
MOCK_CARD_NUMBER = "5555000012348874"
MOCK_CARD_TOKEN  = "15555000000000874"
MOCK_CUSTOMER_NR = "000000000001"


# ─────────────────────────────────────────────────────────────
# CLIENTE HTTP — llama al mock server
# ─────────────────────────────────────────────────────────────

def get_customer() -> dict:
    """
    GET /v1/customer/{customerNumber}
    Devuelve los datos del cliente. El mock siempre retorna James Bond.
    En caso de fallo de red, usa los datos hardcodeados de la documentación.
    """
    try:
        url = f"{MOCK_BASE}/v1/customer/{MOCK_CUSTOMER_NR}"
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[WARN] Mock no disponible, usando datos locales: {e}")
    return MOCK_CUSTOMER


def get_card_transactions() -> list:
    """
    GET /v1/card/{cardNumber}/transactions?lastNbrTransCard=1
    Devuelve las últimas transacciones de la tarjeta.
    """
    try:
        url = f"{MOCK_BASE}/v1/card/{MOCK_CARD_NUMBER}/transactions"
        r = requests.get(url, params={"lastNbrTransCard": 1}, timeout=8)
        if r.status_code == 200:
            data = r.json()
            return data.get("transactions", [MOCK_TRANSACCION])
    except Exception as e:
        print(f"[WARN] Mock transacciones no disponible: {e}")
    return [MOCK_TRANSACCION]


def get_card_rsa() -> dict:
    """
    GET /v1/card/{cardToken}/cardRSAEncrypted
    Devuelve el número de tarjeta y CVV2 encriptados con RSA.
    """
    try:
        url = f"{MOCK_BASE}/v1/card/{MOCK_CARD_TOKEN}/cardRSAEncrypted"
        headers = {"encodedKey": "MOCK_KEY", "includeCVV2": "true"}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[WARN] Mock card RSA no disponible: {e}")
    return MOCK_CARD_RSA


# ─────────────────────────────────────────────────────────────
# VARIACIÓN SINTÉTICA
# Como el mock siempre devuelve los mismos datos, añadimos
# variación controlada para simular múltiples usuarios y
# transacciones con distribuciones estadísticas realistas.
# ─────────────────────────────────────────────────────────────

def variar_usuario(base: dict, iteracion: int) -> dict:
    """
    Toma los datos base de James Bond del mock y genera
    un usuario con identidad variada para cada iteración.
    Mantiene la estructura real de Vertexon.
    """
    promedio = round(random.uniform(30, 800), 2)

    # Coordenadas basadas en la ciudad del mock (New York)
    # y variadas para simular usuarios en distintas ubicaciones
    usuario_x = round(-74.0060 + random.uniform(-20, 20), 6)   # longitud NY ± variación
    usuario_y = round(40.7128 + random.uniform(-15, 15), 6)     # latitud NY ± variación

    return {
        "usuarioID":                  str(uuid.uuid4()),
        "usuario":                    f"{fake.first_name()} {base.get('lastName', 'Bond')}",
        "vertexon_customer_number":   f"{MOCK_CUSTOMER_NR[:-3]}{iteracion:03d}",
        "email":                      fake.email(),
        "fecha_nacimiento":           base.get("dateOfBirth", "1999-01-02"),
        "usuario_x":                  usuario_x,
        "usuario_y":                  usuario_y,
        "promedio_de_gastos":         promedio,
        "varianza_de_gastos":         round(random.uniform(5, promedio * 0.4), 2),
        "promedio_de_gastos_por_dia": round(promedio * random.uniform(0.3, 2.5), 2),
    }


def variar_tarjeta(card_rsa: dict, usuario_id: str) -> dict:
    """
    Toma los datos de la tarjeta del mock.
    El CVV2 viene encriptado RSA por Vertexon — lo almacenamos tal cual.
    La fecha de expiración viene en formato YYMM, la convertimos a MM/YYYY.
    """
    expiry_raw = card_rsa.get("expiryDate", "2612")   # ej: "2612" = dic 2026
    try:
        yy = expiry_raw[:2]
        mm = expiry_raw[2:]
        fecha_exp = f"{mm}/20{yy}"
    except Exception:
        fecha_exp = "12/2026"

    # El CVV2 ya viene encriptado RSA por Vertexon.
    # Si el mock devuelve el placeholder de la doc, encriptamos uno local con Fernet.
    cvv_raw = card_rsa.get("cardCVV2Enc", "")
    if len(cvv_raw) < 20:   # es el placeholder truncado de la documentación
        cvv_raw = cipher.encrypt(str(random.randint(100, 999)).encode()).decode()

    return {
        "tarjetaID":         str(uuid.uuid4()),
        "usuarioID":         usuario_id,
        "card_number_masked": "5555 **** **** " + MOCK_CARD_NUMBER[-4:],
        "fecha_exp_tarjeta": fecha_exp,
        "cvv":               cvv_raw,   # encriptado RSA (Vertexon) o Fernet (mock local)
    }


def variar_transaccion(tx_base: dict, usuario_id: str,
                       tarjeta_id: str, usuario: dict) -> dict:
    """
    Toma la transacción base del mock y genera variación realista:
    - La cantidad se deriva del perfil del usuario (N(promedio, varianza))
    - La fecha/hora es el momento actual (tiempo real)
    - esFraude sigue Bernoulli(0.01) con cantidad inflada si hay fraude
    """
    # Cantidad basada en el perfil del usuario
    cantidad = float(np.random.normal(
        loc=usuario["promedio_de_gastos"],
        scale=np.sqrt(usuario["varianza_de_gastos"])
    ))
    cantidad = max(round(cantidad, 2), 0.01)

    # Fraude: Bernoulli(1%)
    es_fraude = int(np.random.binomial(1, 0.01))
    if es_fraude:
        cantidad = round(cantidad * random.uniform(3, 10), 2)

    # Fecha/hora actual = tiempo real
    ahora = datetime.now()

    return {
        "transaccionID":                 str(uuid.uuid4()),
        "fecha_hora_transaccion":        ahora.strftime("%Y-%m-%d %H:%M:%S"),
        "usuarioID":                     usuario_id,
        "terminalID":                    tx_base.get("cardToken", MOCK_CARD_TOKEN),
        "tarjetaID":                     tarjeta_id,
        "cantidad_transaccion":          cantidad,
        "tiempo_promedio_x_transaccion": round(TIEMPO_PROMEDIO_TX, 4),
        "esFraude":                      es_fraude,
        # campos adicionales de Vertexon
        "descripcion":      tx_base.get("transDescription", ""),
        "producto":         tx_base.get("product", "MasterCard Elite"),
        "cuenta_origen":    tx_base.get("accountNbr", "00000000001"),
        "moneda":           tx_base.get("transactionAmount", {}).get("currencyCode", "840"),
    }


# ─────────────────────────────────────────────────────────────
# KAFKA
# ─────────────────────────────────────────────────────────────

def crear_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all",
    )


def publicar(producer: KafkaProducer, topic: str, key: str, mensaje: dict):
    try:
        producer.send(topic, key=key, value=mensaje).get(timeout=10)
    except KafkaError as e:
        print(f"[ERROR] Kafka → {topic}: {e}")


# ─────────────────────────────────────────────────────────────
# LOOP PRINCIPAL — tiempo real
# ─────────────────────────────────────────────────────────────

def run(intervalo: float = 2.0, variacion: int = 5):
    """
    Cada `intervalo` segundos:
      1. Consulta el mock de Vertexon (customer, card, transactions)
      2. Genera `variacion` usuarios sintéticos basados en los datos del mock
      3. Publica todo a Kafka en tiempo real

    Args:
        intervalo : segundos entre cada ciclo de publicación
        variacion : cuántos usuarios sintéticos generar por ciclo
    """
    producer = crear_kafka_producer()
    print(f"✔  Kafka         : {KAFKA_BROKER}")
    print(f"✔  Mock server   : {MOCK_BASE}")
    print(f"✔  Intervalo     : {intervalo} s")
    print(f"✔  Usuarios/ciclo: {variacion}")
    print(f"✔  Tiempo prom/tx: {TIEMPO_PROMEDIO_TX:.4f} s")
    print("\nPresiona Ctrl+C para detener.\n")

    total_tx    = 0
    total_fraud = 0
    ciclo       = 0

    try:
        while True:
            ciclo += 1

            # 1. Llamar al mock una vez por ciclo
            customer_base = get_customer()
            txs_base      = get_card_transactions()
            card_rsa      = get_card_rsa()
            tx_base       = txs_base[0] if txs_base else MOCK_TRANSACCION

            # 2. Generar `variacion` usuarios distintos por ciclo
            for i in range(variacion):
                iteracion = (ciclo - 1) * variacion + i

                # ── Usuario ──────────────────────────────────
                usuario = variar_usuario(customer_base, iteracion)
                publicar(producer, TOPIC_USUARIOS, usuario["usuarioID"], usuario)

                # ── Tarjeta ──────────────────────────────────
                tarjeta = variar_tarjeta(card_rsa, usuario["usuarioID"])
                publicar(producer, TOPIC_TARJETAS, tarjeta["tarjetaID"], tarjeta)

                # ── Transacción ──────────────────────────────
                tx = variar_transaccion(tx_base, usuario["usuarioID"],
                                        tarjeta["tarjetaID"], usuario)
                publicar(producer, TOPIC_TRANSACCIONES, tx["transaccionID"], tx)

                total_tx    += 1
                total_fraud += tx["esFraude"]

            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"ciclo={ciclo:04d} | "
                f"txs_totales={total_tx} | "
                f"fraudes={total_fraud} ({total_fraud/total_tx*100:.1f}%)"
            )

            time.sleep(intervalo)

    except KeyboardInterrupt:
        print("\n\nDetenido.")
    finally:
        producer.flush()
        producer.close()
        print(f"\n{'═'*40}")
        print(f"  Ciclos ejecutados: {ciclo}")
        print(f"  Transacciones    : {total_tx}")
        print(f"  Fraudes          : {total_fraud}")
        print(f"  Tiempo prom/tx   : {TIEMPO_PROMEDIO_TX:.4f} s")
        print(f"{'═'*40}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vertexon Mock → Kafka Producer")
    parser.add_argument(
        "--intervalo", type=float, default=2.0,
        help="Segundos entre cada ciclo (default: 2)"
    )
    parser.add_argument(
        "--variacion", type=int, default=5,
        help="Usuarios sintéticos por ciclo (default: 5)"
    )
    args = parser.parse_args()
    run(args.intervalo, args.variacion)