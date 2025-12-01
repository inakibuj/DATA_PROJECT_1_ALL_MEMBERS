import json
import time
import os
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json

# --- CONFIGURACIÓN ---
KAFKA_SERVER = "kafka:9092"
TOPIC_POLLUTION = "valencia_pollution"
TOPIC_WEATHER = "valencia_weather"

# CONFIGURACIÓN POSTGRES
PG_HOST = "postgres_dp1"
PG_DB = "calidad_aire"
PG_USER = "admin"
PG_PASS = "admin"

# --- LÍMITES OFICIALES (Configuración Científica) ---
# Basado en umbrales de alerta o calidad "Desfavorable" (µg/m³)
LIMITES_CALIDAD = {
    'no2': {'limite': 200, 'desc': 'Dióxido de Nitrógeno'},
    'pm10': {'limite': 50, 'desc': 'Partículas PM10'},
    'pm25': {'limite': 25, 'desc': 'Partículas PM2.5'},
    'o3': {'limite': 180, 'desc': 'Ozono'},
    'so2': {'limite': 350, 'desc': 'Dióxido de Azufre'},
    'co': {'limite': 10, 'desc': 'Monóxido de Carbono'} # mg/m3
}

def get_db_connection():
    conn = None
    while not conn:
        try:
            conn = psycopg2.connect(
                host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS
            )
            print("✅ Conexión exitosa con Postgres")
        except Exception as e:
            print(f"⏳ Esperando a Postgres... ({e})")
            time.sleep(5)
    return conn

def create_tables(conn):
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_pollution (
                id SERIAL PRIMARY KEY,
                ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data JSONB
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_weather (
                id SERIAL PRIMARY KEY,
                ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data JSONB
            );
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"❌ Error creando tablas: {e}")

def get_kafka_consumer():
    consumer = None
    while not consumer:
        try:
            print("📡 Intentando conectar con Kafka...")
            consumer = KafkaConsumer(
                TOPIC_POLLUTION, TOPIC_WEATHER,
                bootstrap_servers=[KAFKA_SERVER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='grupo_valencia_leector_v2', # Cambio ID para releer si hace falta
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("✅ Conexión exitosa con Kafka")
        except Exception as e:
            print(f"⏳ Kafka aún no está listo, reintentando en 5s...")
            time.sleep(5)
    return consumer

# --- FUNCIÓN DE ALERTA CIENTÍFICA ---
def check_and_alert(data, topic):
    if topic != TOPIC_POLLUTION:
        return

    station_name = data.get('nombre', 'Estación desconocida')
    
    # Recorremos todos los contaminantes que vigilamos
    alerts_triggered = []
    
    for contaminante, info in LIMITES_CALIDAD.items():
        valor = data.get(contaminante)
        
        # Verificamos que el valor exista y sea numérico
        if valor is not None and isinstance(valor, (int, float)):
            if valor > info['limite']:
                alerts_triggered.append(f"{info['desc']}: {valor} (Límite: {info['limite']})")

    # Si hay alertas, las mostramos
    if alerts_triggered:
        print("\n" + "!"*60)
        print(f"🚨 ALERTA TOXICOLÓGICA REAL 🚨")
        print(f"📍 Estación: {station_name}")
        for alerta in alerts_triggered:
            print(f"⚠️  {alerta}")
        print("!"*60 + "\n")
    else:
        # Opcional: Mostrar que está limpio para saber que funciona
        # Cogemos el NO2 como referencia rápida
        no2 = data.get('no2', 0)
        print(f"💚 Aire Normativo en {station_name} (NO2: {no2} < 200)")

def main():
    conn = get_db_connection()
    create_tables(conn)
    consumer = get_kafka_consumer()
    cur = conn.cursor()

    print("👀 Vigilando todos los parámetros de calidad del aire...")
    
    for message in consumer:
        data = message.value
        topic = message.topic
        
        try:
            check_and_alert(data, topic)

            table_name = "raw_pollution" if topic == TOPIC_POLLUTION else "raw_weather"
            cur.execute(
                f"INSERT INTO {table_name} (data) VALUES (%s)",
                (Json(data),)
            )
            conn.commit()

        except Exception as e:
            print(f"❌ Error procesando: {e}")
            conn.rollback()

if __name__ == "__main__":
    main()