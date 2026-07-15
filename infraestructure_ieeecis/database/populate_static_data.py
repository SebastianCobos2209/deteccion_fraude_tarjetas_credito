"""
================================================================================
SCRIPT: populate_static_data.py
PROPOSITO: Poblacion de colecciones estaticas (user_profiles y cards) en MongoDB.
TIPO DE ARCHIVO: Inicializacion de Base de Datos / Infraestructura
================================================================================

DESCRIPCION:
Este script lee el archivo 'train_cleaned.csv' para agrupar y extraer las 
combinaciones de usuarios y tarjetas mas frecuentes del dataset. A partir de ahi:
1. Genera 5,000 perfiles de usuario ficticios (user_profiles) con nombres, 
   emails, coordenadas y estadisticas de gasto promedio y varianza.
2. Genera 5,000 tarjetas asociadas (cards) enmascaradas y estructuradas.
3. Se conecta al contenedor de MongoDB en localhost:27018.
4. Vacia e inserta estos registros en las colecciones correspondientes.

ORDEN DE EJECUCION (FASE DE INFRAESTRUCTURA / PREPARACION):
1. Iniciar Docker Compose (Zookeeper, Kafka, MongoDB, etc.).
2. EJECUTAR ESTE SCRIPT (`populate_static_data.py`) para establecer los datos base.
3. Ejecutar `populate_dynamic_data.py` para inyectar transacciones historicas.
================================================================================
"""

import os
import random
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pymongo import MongoClient

def populate_static_data():
    print("="*60)
    print("Inicialización de Colecciones Estáticas en MongoDB (user_profiles / cards)")
    print("="*60)

    # 1. Establecer rutas
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cleaned_path = os.path.join(current_dir, '..', 'data_insight', 'data_ieee', 'train_cleaned.csv')

    if not os.path.exists(cleaned_path):
        print(f"[Error] No se encontró el dataset limpio en: {cleaned_path}")
        print("Asegúrate de haber ejecutado el notebook del EDA primero.")
        return

    # 2. Conectar a MongoDB
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

    # 3. Cargar columnas clave de train_cleaned.csv
    print("[2/5] Cargando columnas de train_cleaned.csv (optimizado)...")
    cols_to_load = [
        "card1", "card4", "card6", "ProductCD", "P_emaildomain", 
        "addr1", "addr2", "DeviceType", "DeviceInfo", "TransactionAmt"
    ]
    df = pd.read_csv(cleaned_path, usecols=cols_to_load)
    print(f"  -> Registros leídos: {len(df)}")

    # Rellenar nulos para agrupación consistente
    print("  -> Rellenando valores nulos para categorización...")
    df["card4"] = df["card4"].fillna("unknown")
    df["card6"] = df["card6"].fillna("unknown")
    df["ProductCD"] = df["ProductCD"].fillna("W")
    df["P_emaildomain"] = df["P_emaildomain"].fillna("unknown.com")
    df["addr1"] = df["addr1"].fillna(999.0)
    df["addr2"] = df["addr2"].fillna(99.0)
    df["DeviceType"] = df["DeviceType"].fillna("unknown")
    df["DeviceInfo"] = df["DeviceInfo"].fillna("unknown")

    # 4. Agrupar para identificar perfiles de usuario únicos
    print("[3/5] Identificando combinaciones únicas de usuarios/tarjetas...")
    grouped = df.groupby([
        "card1", "card4", "card6", "ProductCD", "P_emaildomain", 
        "addr1", "addr2", "DeviceType", "DeviceInfo"
    ])
    
    # Calcular promedio, varianza y cantidad de gastos
    user_stats = grouped["TransactionAmt"].agg(["mean", "var", "count"]).reset_index()
    
    # Ordenar por el número de transacciones para priorizar usuarios reales más activos
    user_stats = user_stats.sort_values(by="count", ascending=False)
    
    # Limitamos a un número manejable y representativo (5000 usuarios)
    limit = 5000
    user_stats = user_stats.head(limit)
    print(f"  -> Perfiles seleccionados: {len(user_stats)}")

    # 5. Generar listas de documentos para MongoDB
    print("[4/5] Generando documentos user_profiles y cards...")
    user_profiles_list = []
    cards_list = []

    first_names = [
        "Juan", "Maria", "Jose", "Ana", "Luis", "Carlos", "Laura", "Pedro", "Sofia", "Jorge", 
        "Elena", "Diego", "Carmen", "Javier", "Marta", "Sebastian", "Jeaneth", "Gabriel", "Lucia", "Andres"
    ]
    last_names = [
        "Cobos", "Gomez", "Rodriguez", "Lopez", "Perez", "Gonzalez", "Martinez", "Sanchez", "Alvarez", "Fernandez", 
        "Torres", "Ramirez", "Cruz", "Ortiz", "Flores", "Benitez", "Silva", "Castro", "Rios", "Morales"
    ]

    random.seed(42) # Reproducible

    for idx, row in user_stats.iterrows():
        # Generar IDs estructurados
        usr_id = f"USR_{idx:06d}"
        card_id = f"CRD_{idx:06d}"
        
        # Generar nombre y email ficticio
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        email_domain = row["P_emaildomain"]
        if email_domain == "unknown.com":
            email_domain = "gmail.com"
        email = f"{name.lower().replace(' ', '.')}@{email_domain}"
        
        # Fecha nacimiento (entre 18 y 65 años atrás)
        age_days = random.randint(18*365, 65*365)
        birth_date = datetime.now() - timedelta(days=age_days)
        birth_date_str = birth_date.strftime("%Y-%m-%d")
        
        # Coordenadas geográficas ficticias basadas en el país (addr2)
        # 87.0 suele ser US en dataset de Vesta.
        if row["addr2"] == 87.0:
            lat = random.uniform(25.0, 49.0)
            lon = random.uniform(-125.0, -70.0)
        else:
            # Latitud/longitud simuladas para Ecuador u otros países
            lat = random.uniform(-2.0, 1.0)
            lon = random.uniform(-80.0, -75.0)

        promedio = float(row["mean"])
        varianza = float(row["var"]) if not pd.isna(row["var"]) else 0.0

        # Crear perfil del usuario
        profile = {
            "usuarioID": usr_id,
            "usuario": name,
            "email": email,
            "fecha_nacimiento": birth_date_str,
            "usuario_x": float(lon),
            "usuario_y": float(lat),
            "addr1": float(row["addr1"]),
            "addr2": float(row["addr2"]),
            "DeviceType": str(row["DeviceType"]),
            "DeviceInfo": str(row["DeviceInfo"]),
            "promedio_de_gastos": promedio,
            "varianza_de_gastos": varianza,
            "promedio_gastos_por_dia": 1.5,
            "vertexon_customer_number": f"VTX-{random.randint(10000, 99999)}"
        }
        user_profiles_list.append(profile)

        # Crear tarjeta del usuario
        card_number = f"{random.randint(1000, 9999)}******{random.randint(1000, 9999)}"
        exp_year = random.randint(2027, 2033)
        exp_month = random.randint(1, 12)
        exp_date = f"{exp_month:02d}/{exp_year}"
        cvv_mock = f"cvv_{random.randint(100, 999)}" # Encriptado simulado

        card = {
            "tarjetaID": card_id,
            "usuarioID": usr_id,
            "card_number_masked": card_number,
            "fecha_exp_tarjeta": exp_date,
            "cvv": cvv_mock,
            "card1": int(row["card1"]),
            "card4": str(row["card4"]),
            "card6": str(row["card6"]),
            "ProductCD": str(row["ProductCD"])
        }
        cards_list.append(card)

    # 6. Guardar en MongoDB (borrar contenido previo)
    print("[5/5] Escribiendo colecciones en MongoDB...")
    
    # Limpiar tablas
    db.user_profiles.delete_many({})
    db.cards.delete_many({})

    # Bulk insert
    if user_profiles_list:
        db.user_profiles.insert_many(user_profiles_list)
    if cards_list:
        db.cards.insert_many(cards_list)

    print("\n¡Carga de datos estáticos completada con éxito!")
    print(f"  -> user_profiles insertados: {db.user_profiles.count_documents({})}")
    print(f"  -> cards insertados: {db.cards.count_documents({})}")
    print("="*60)

if __name__ == "__main__":
    populate_static_data()
