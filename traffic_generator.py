import random
import time
import json
from datetime import datetime
import mysql.connector
from kafka import KafkaProducer
import numpy as np

# Configuración
KAFKA_SERVER = "kafka:9092"
MYSQL_HOST = "mysql"
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "waze_db"
QUERY_TOPIC = "traffic-queries"
CACHE_TOPIC = "cache-updates"

# Conexión a MySQL
def connect_mysql():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

# Productor Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_poisson_traffic(rate_per_sec, duration_sec):
    """Genera tráfico con distribución Poisson"""
    queries = []
    timestamps = np.random.exponential(1/rate_per_sec, size=int(rate_per_sec*duration_sec*1.5))
    current_time = 0
    for delta in timestamps:
        current_time += delta
        if current_time > duration_sec:
            break
        queries.append(current_time)
    return queries

def generate_uniform_traffic(rate_per_sec, duration_sec):
    """Genera tráfico con distribución Uniforme"""
    interval = 1/rate_per_sec
    return [i*interval for i in range(int(rate_per_sec*duration_sec))]

def get_random_event_from_db():
    """Obtiene un evento aleatorio de la base de datos"""
    try:
        conn = connect_mysql()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM events ORDER BY RAND() LIMIT 1")
        event = cursor.fetchone()
        cursor.close()
        conn.close()
        return event
    except Exception as e:
        print(f"Error getting event from DB: {e}")
        return None

def run_traffic_generator():
    print("🚦 Iniciando generador de tráfico...")
    
    # Configuración de patrones de tráfico
    patterns = [
        {"name": "Poisson (Alta)", "func": generate_poisson_traffic, "rate": 10},
        {"name": "Poisson (Baja)", "func": generate_poisson_traffic, "rate": 2},
        {"name": "Uniforme", "func": generate_uniform_traffic, "rate": 5}
    ]
    
    while True:
        # Seleccionar un patrón aleatorio cada 2 minutos
        pattern = random.choice(patterns)
        print(f"\n🔁 Cambiando a patrón: {pattern['name']} (tasa: {pattern['rate']}/s)")
        
        # Generar tiempos de consulta según el patrón seleccionado
        query_times = pattern["func"](pattern["rate"], 120)  # 2 minutos por patrón
        
        for query_time in query_times:
            time.sleep(query_time)
            event = get_random_event_from_db()
            
            if event:
                # Crear consulta con metadata adicional
                query = {
                    "timestamp": datetime.now().isoformat(),
                    "event_id": event["id"],
                    "event_type": event["event_type"],
                    "location": {
                        "lat": event["latitude"],
                        "lon": event["longitude"]
                    },
                    "query_pattern": pattern["name"]
                }
                
                # Enviar a Kafka (para el sistema de caché)
                producer.send(QUERY_TOPIC, query)
                print(f"📤 Consulta enviada: Evento {event['id']} ({event['event_type']})")

if __name__ == "__main__":
    run_traffic_generator()