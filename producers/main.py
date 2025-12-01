import time
import json
import requests
from kafka import KafkaProducer
import schedule
from datetime import datetime

# --- CONFIGURACIÓN ---
# Usamos el dominio oficial de OpenDataSoft Valencia
BASE_URL = "https://valencia.opendatasoft.com"

# URL 1: Contaminación
URL_POLLUTION = f"{BASE_URL}/api/explore/v2.1/catalog/datasets/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/records?limit=20"

# URL 2: Meteorología (Clima)
URL_WEATHER = f"{BASE_URL}/api/explore/v2.1/catalog/datasets/estacions-atmosferiques-estaciones-atmosfericas/records?limit=20"

# Configuración Kafka
KAFKA_SERVER = "kafka:9092"

# Definimos dos tópicos diferentes para mantener el orden
TOPIC_POLLUTION = "valencia_pollution"
TOPIC_WEATHER = "valencia_weather"

def get_kafka_producer():
    # Intentamos conectar hasta que Kafka esté listo (Docker a veces tarda en arrancar)
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("✅ Conexión exitosa con Kafka")
        except Exception as e:
            print(f"⏳ Esperando a Kafka... ({e})")
            time.sleep(5)
    return producer

producer = get_kafka_producer()

def fetch_and_send(url, topic_name, data_type):
    """
    Función genérica para descargar de la API y enviar a Kafka
    """
    print(f"📥 Consultando API de {data_type}...")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            # La API devuelve una lista dentro de 'results'
            records = data.get('results', [])
            
            print(f"   -> Encontrados {len(records)} registros.")
            
            for record in records:
                # Añadimos marca de tiempo de ingestión
                record['ingestion_timestamp'] = datetime.now().isoformat()
                record['data_type'] = data_type # Etiqueta extra por si acaso
                
                # ENVIAR A KAFKA
                producer.send(topic_name, value=record)
            
            producer.flush()
            print(f"📤 Datos de {data_type} enviados a Kafka correctamente.")
            
        else:
            print(f"❌ Error HTTP {response.status_code} al consultar {data_type}")
            
    except Exception as e:
        print(f"❌ Error de conexión: {e}")

def job():
    print(f"\n--- INICIO CICLO {datetime.now().strftime('%H:%M:%S')} ---")
    # 1. Traer Contaminación
    fetch_and_send(URL_POLLUTION, TOPIC_POLLUTION, "contaminacion")
    
    # 2. Traer Clima
    fetch_and_send(URL_WEATHER, TOPIC_WEATHER, "clima")
    print("--- FIN CICLO ---\n")

# --- EJECUCIÓN ---
# Ejecutar una vez al arrancar
job()

# Programar cada 10 minutos (600 segundos)
schedule.every(10).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
    