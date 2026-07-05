"""
================================================================================
SCRIPT: populate_dynamic_data.py
PROPOSITO: Poblacion de colecciones dinamicas (transactions_enriched / fraud_alerts) en MongoDB.
TIPO DE ARCHIVO: Inicializacion de Base de Datos / Infraestructura
================================================================================

DESCRIPCION:
Este script genera un historico simulado de transacciones financieras y alertas
para alimentar el dashboard de Grafana. A partir de los usuarios (user_profiles)
y tarjetas (cards) ya existentes en MongoDB:
1. Genera 2,500 transacciones simuladas distribuidas en los ultimos 7 dias.
2. Inyecta una tasa de fraude simulado de aproximadamente 5% para que sea visualmente
   interesante en el dashboard (montos elevados, variables alteradas).
3. Genera alertas de fraude asociadas (`fraud_alerts`) simulando el workflow de los
   analistas (estados "confirmed_fraud", "dismissed", "blocked", "flagged").
4. Vacia e inserta estos registros de forma masiva (Bulk Write) en MongoDB.

ORDEN DE EJECUCION (FASE DE INFRAESTRUCTURA / PREPARACION):
1. Iniciar Docker Compose.
2. Ejecutar `populate_static_data.py`.
3. EJECUTAR ESTE SCRIPT (`populate_dynamic_data.py`).
================================================================================
"""

import os
import random
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient

def populate_dynamic_data():
    print("="*60)
    print("Población de Datos Dinámicos en MongoDB (transactions_enriched / fraud_alerts)")
    print("="*60)

    # 1. Conectar a MongoDB
    print("[1/5] Conectando a MongoDB...")
    try:
        client = MongoClient("mongodb://admin:tfm2026@localhost:27018/?authSource=admin", serverSelectionTimeoutMS=5000)
        db = client["fraude_db"]
        # Validar conexión
        client.admin.command('ping')
        print("  -> Conexión exitosa a MongoDB.")
    except Exception as e:
        print(f"[Error] No se pudo conectar a MongoDB: {e}")
        print("Asegúrate de que los contenedores Docker estén corriendo (docker compose up -d).")
        return

    # 2. Cargar perfiles de usuario y tarjetas existentes
    print("[2/5] Recuperando perfiles estáticos de la base de datos...")
    users = list(db.user_profiles.find({}))
    cards = list(db.cards.find({}))

    if not users or not cards:
        print("[Error] No se encontraron perfiles de usuario o tarjetas.")
        print("Asegúrate de ejecutar primero populate_static_data.py para llenar los datos estáticos.")
        return

    print(f"  -> Perfiles de usuario cargados: {len(users)}")
    print(f"  -> Tarjetas cargadas: {len(cards)}")

    # Mapeo rápido de tarjetas por usuario
    cards_by_user = {c["usuarioID"]: c for c in cards}

    # 3. Limpiar colecciones dinámicas existentes
    print("[3/5] Limpiando colecciones dinámicas existentes...")
    db.transactions_raw.delete_many({})
    db.transactions_enriched.delete_many({})
    db.fraud_alerts.delete_many({})
    print("  -> Limpieza completada.")

    # 4. Generación de transacciones sintéticas históricas (últimos 7 días)
    print("[4/5] Generando 2,500 transacciones distribuidas en los últimos 7 días...")
    
    num_transactions = 2500
    start_date = datetime.now() - timedelta(days=7)
    
    # Lista para inserción en masa (Bulk insert)
    raw_list = []
    enriched_list = []
    alerts_list = []

    analistas = ["analista_jeaneth", "analista_carlos", "analista_sofia"]
    random.seed(42)
    np.random.seed(42)

    for idx in range(num_transactions):
        tx_id = f"TX_{idx:08d}"
        
        # Elegir un usuario aleatorio
        user = random.choice(users)
        usr_id = user["usuarioID"]
        card = cards_by_user.get(usr_id)
        
        if not card:
            # Si no tiene tarjeta, saltar
            continue
            
        card_id = card["tarjetaID"]
        
        # Generar marca de tiempo distribuida en los últimos 7 días
        seconds_offset = random.randint(0, 7 * 24 * 3600)
        txn_time = start_date + timedelta(seconds=seconds_offset)
        
        # Simular si la transacción es fraudulenta (Tasa de contaminación: 5% para que sea visualmente atractivo en Grafana)
        is_fraud = 1 if random.random() < 0.05 else 0
        
        # Parámetros del monto
        avg_spent = user.get("promedio_de_gastos", 80.0)
        std_spent = np.sqrt(user.get("varianza_de_gastos", 400.0))
        if std_spent < 5.0:
            std_spent = 20.0
            
        if is_fraud:
            # Fraude: Monto inusual, mucho mayor que la media
            amt = float(avg_spent * random.uniform(3.0, 6.0) + random.uniform(50.0, 200.0))
            fraud_score = float(random.uniform(0.65, 0.95))
            is_suspicious = True
            
            # Variables de comportamiento alteradas
            c1 = float(random.randint(15, 50))
            c13 = float(random.randint(40, 150))
            c7 = float(random.randint(5, 20))
            c14 = float(random.randint(10, 40))
            d1 = float(random.randint(100, 360))
            
            # V variables alteradas
            v257 = float(random.uniform(3.0, 10.0))
            v258 = float(random.uniform(3.0, 10.0))
            v314 = float(amt)
            
            zscore_amt = float(random.uniform(3.5, 8.5))
            velocity = float(random.randint(5, 15)) # Muchas transacciones seguidas
            amt_distance = float(abs(amt - avg_spent))
        else:
            # Normal: Siguiendo su distribución normal
            amt = float(max(1.0, np.random.normal(avg_spent, std_spent)))
            fraud_score = float(random.uniform(0.01, 0.28))
            is_suspicious = False
            
            # Variables de comportamiento normales
            c1 = float(random.randint(1, 4))
            c13 = float(random.randint(1, 15))
            c7 = float(0.0)
            c14 = float(random.randint(1, 5))
            d1 = float(random.randint(0, 90))
            
            v257 = float(1.0)
            v258 = float(1.0)
            v314 = float(0.0)
            
            # Zscore real
            zscore_amt = float((amt - avg_spent) / (std_spent + 1e-9))
            velocity = float(random.randint(1, 3))
            amt_distance = float(abs(amt - avg_spent))

        # 4.1 Transacción Cruda
        raw_txn = {
            "transaccionID": tx_id,
            "usuarioID": usr_id,
            "tarjetaID": card_id,
            "fecha_hora_transaccion": txn_time,
            "TransactionAmt": amt,
            "TransactionDT": int(txn_time.timestamp()),
            "isFraud": is_fraud
        }
        raw_list.append(raw_txn)

        # 4.2 Transacción Enriquecida
        enriched_txn = {
            "transaccionID": tx_id,
            "usuarioID": usr_id,
            "tarjetaID": card_id,
            "TransactionAmt": amt,
            "TransactionDT": int(txn_time.timestamp()),
            "card1": int(card["card1"]),
            "card4": str(card["card4"]),
            "card6": str(card["card6"]),
            "ProductCD": str(card["ProductCD"]),
            "P_emaildomain": str(user["email"].split('@')[-1]),
            "addr1": float(user["addr1"]),
            "addr2": float(user["addr2"]),
            "DeviceType": str(user["DeviceType"]),
            "DeviceInfo": str(user["DeviceInfo"]),
            
            # Controles C y delta D
            "C1": c1, "C13": c13, "C7": c7, "C14": c14,
            "D1": d1,
            
            # Variables V
            "V314": v314,
            "V201": float(random.choice([0.0, 1.0, 2.0])),
            "V243": float(random.choice([0.0, 1.0, 2.0])),
            "V257": v257,
            "V242": float(random.choice([0.0, 1.0, 2.0])),
            "V45": float(random.choice([0.0, 1.0, 2.0])),
            "V246": float(random.choice([0.0, 1.0, 2.0])),
            "V200": float(random.choice([0.0, 1.0, 2.0])),
            "V258": v258,
            
            # Enriquecidos analíticos
            "zscore_amt": zscore_amt,
            "velocity": velocity,
            "amt_distance": amt_distance,
            
            # Predicción/Inferencia
            "fraud_score": fraud_score,
            "is_suspicious": is_suspicious,
            "model_ready": True,
            "processed_at": txn_time
        }
        enriched_list.append(enriched_txn)

        # 4.3 Alertas de Fraude
        if is_suspicious:
            # Decidir el estado del workflow (70% gestionadas, 30% pendientes)
            status_rand = random.random()
            if status_rand < 0.7:
                # Alerta gestionada
                status = random.choice(["confirmed_fraud", "dismissed", "blocked"])
                reviewed_by = random.choice(analistas)
                # Resuelta entre 5 y 60 minutos después de la alerta
                minutes_to_review = random.randint(5, 60)
                reviewed_at = txn_time + timedelta(minutes=minutes_to_review)
            else:
                # Alerta pendiente
                status = "flagged"
                reviewed_by = None
                reviewed_at = None

            alert = {
                "transaccionID": tx_id,
                "usuarioID": usr_id,
                "tarjetaID": card_id,
                "TransactionAmt": amt,
                "fraud_score": fraud_score,
                "zscore_amt": zscore_amt,
                "velocity": velocity,
                "isFraud_label": is_fraud,
                "alerted_at": txn_time,
                "status": status,
                "reviewed_by": reviewed_by,
                "reviewed_at": reviewed_at
            }
            alerts_list.append(alert)

    # 5. Guardar en MongoDB usando bulk write
    print("[5/5] Escribiendo datos en MongoDB...")
    
    if raw_list:
        db.transactions_raw.insert_many(raw_list)
    if enriched_list:
        db.transactions_enriched.insert_many(enriched_list)
    if alerts_list:
        db.fraud_alerts.insert_many(alerts_list)

    print("\n¡Población de datos dinámicos completada con éxito!")
    print(f"  -> transactions_raw insertados     : {db.transactions_raw.count_documents({})}")
    print(f"  -> transactions_enriched insertados: {db.transactions_enriched.count_documents({})}")
    print(f"  -> fraud_alerts insertados         : {db.fraud_alerts.count_documents({})}")
    print("="*60)

if __name__ == "__main__":
    populate_dynamic_data()
