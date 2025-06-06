import random
import time
import json
from datetime import datetime
import mysql.connector
from kafka import KafkaProducer
import math

# Configuracion
KAFKA_SERVER = "kafka:9092"
MYSQL_HOST = "mysql"
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "waze_db"
QUERY_TOPIC = "traffic-queries"
WAIT_INTERVAL = 5  
# Eventos minimos para iniciar el generador de tráfico
MIN_EVENTS = 100000    

# Conexion MySQL
def connect_mysql():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

# Configuracion del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Obtener todos los eventos de la base de datos
def get_all():
    try:
        conn = connect_mysql()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id, event_type, latitude, longitude FROM events")
        events = cursor.fetchall()
        cursor.close()
        conn.close()
        return events
    except Exception as e:
        print(f"⚠️ Error getting events from DB: {e}")
        return None

# Esperar a que el scraper cargue datos iniciales (ver parametro MIN_EVENTS)
def wait_for_initial_data():
    print("⏳ Esperando a que el scraper cargue datos iniciales...")
    while True:
        try:
            conn = connect_mysql()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM events")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            if count >= MIN_EVENTS:
                print(f"✅ Suficientes eventos cargados ({count} eventos). Empezando generación de tráfico...")
                return True
            else:
                print(f"🕒 Esperando más eventos... ({count}/{MIN_EVENTS})")
                time.sleep(WAIT_INTERVAL)
        except Exception as e:
            print(f"⚠️ Error verificando eventos iniciales: {e}")
            time.sleep(WAIT_INTERVAL)
    return False

# Crear perfil de frecuencias
def create_frequency_profile(events, distribution):
    event_ids = [event['id'] for event in events]
    n = len(event_ids)
    
    if distribution == "uniform":
        weights = [1.0 for _ in event_ids]
    elif distribution == "logarithmic":
        weights = [1.0/(math.log(i+1)+1) for i in range(n)]
    else:
        raise ValueError("Distribución no soportada")
    
    total = sum(weights)
    normalized_weights = [w/total for w in weights]
    
    return {event['id']: {'event': event, 'weight': weight} 
            for event, weight in zip(events, normalized_weights)}

# Generar trafico basado en el perfil de frecuencias
def generate_traffic(events, frequency_profile, pattern_name, duration_sec=60):

    print(f"\n🔁 Patrón de tráfico: {pattern_name}")
    print(f"• Distribución: {'Logarítmica' if 'log' in pattern_name.lower() else 'Uniforme'}")
    print(f"• Duración: {duration_sec} segundos")
    
    # Obtener IDs de eventos y pesos
    event_ids = [eid for eid in frequency_profile.keys()]
    weights = [fp['weight'] for fp in frequency_profile.values()]
    event_map = {eid: fp['event'] for eid, fp in frequency_profile.items()}
    
    start_time = time.time()
    query_count = 0
    
    while time.time() - start_time < duration_sec:

        selected_id = random.choices(event_ids, weights=weights, k=1)[0]
        event = event_map[selected_id]
        
        # Crear consulta
        query = {
            "timestamp": datetime.now().isoformat(),
            "event_id": event["id"],
            "event_type": event["event_type"],
            "location": {
                "lat": event["latitude"],
                "lon": event["longitude"]
            },
            "query_pattern": pattern_name,
            "distribution": "logarithmic" if "log" in pattern_name.lower() else "uniform"
        }
        
        # Enviar consulta a Kafka
        producer.send(QUERY_TOPIC, query)
        query_count += 1
        print(f"📤 Consulta {query_count}: Evento {event['id']} ({event['event_type']})") 
        # Simular tiempo de espera entre consultas
        time.sleep(0.01)  
    
    print(f"✅ Fin patrón {pattern_name}. Total consultas: {query_count}")
    return query_count

# Función principal: inicia el generador de trafico y muestra las estadisticas
def run_traffic_generator():
    print("🚦 Iniciando generador de tráfico con distribución de frecuencias...")
    
    if not wait_for_initial_data():
        print("❌ No se pudo cargar datos iniciales")
        return
    
    events = get_all()
    if not events:
        print("❌ No se encontraron eventos en la base de datos")
        return
    
    print(f"📊 Total de eventos disponibles: {len(events)}")
    
    # Crear perfiles de frecuencias
    uniform_profile = create_frequency_profile(events, "uniform")
    log_profile = create_frequency_profile(events, "logarithmic")
    
    # Mostrar perfiles de frecuencias
    print("\n📈 Perfil de frecuencias (Logarítmico):")
    sample_ids = random.sample(list(log_profile.keys()), 5)
    for eid in sample_ids:
        print(f"• Evento {eid}: Peso {log_profile[eid]['weight']:.6f}")
    
    print("\n📊 Perfil de frecuencias (Uniforme):")
    sample_ids = random.sample(list(uniform_profile.keys()), 5)
    for eid in sample_ids:
        print(f"• Evento {eid}: Peso {uniform_profile[eid]['weight']:.6f}")
    
    # Definir patrones de tráfico
    patterns = [
        ("uniform", uniform_profile),
        ("logarithmic", log_profile)
    ]
    
    while True:
        for pattern_name, profile in patterns:
            
            # Generar tráfico con el perfil de frecuencias
            generate_traffic(
                events=events,
                frequency_profile=profile,
                pattern_name=pattern_name,
                duration_sec=900 # 15 minutos
            )
            
            time.sleep(500)

if __name__ == "__main__":
    run_traffic_generator()