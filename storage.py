import json
import time
import mysql.connector
from kafka import KafkaConsumer

# Configuracion
TOPIC_NAME = "waze-events"
KAFKA_SERVER = "kafka:9092"
MYSQL_HOST = "mysql"
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "waze_db"

# conexion a MySQL
def conect():
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )
            return conn
        except mysql.connector.Error as err:
            print(f"⚠️ Intento {attempt + 1} de {max_retries}: Error conectando a MySQL: {err}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

# Crear tabla si no existe
def crear_tabla(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME NOT NULL,
                latitude DOUBLE NOT NULL,
                longitude DOUBLE NOT NULL,
                event_type VARCHAR(255) NOT NULL
            )
        """)
        print("✅ Tabla 'events' creada/verificada")
    except mysql.connector.Error as err:
        print(f"⚠️ Error creando tabla: {err}")
        raise

# Crear consumidor de Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_SERVER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='storage-group'
)

print("⏳ Conectando a MySQL...")
try:
    # Conectar a MySQL
    conn = conect()
    cursor = conn.cursor()
    
    cursor.execute("SHOW TABLES LIKE 'events'")
    result = cursor.fetchone()
        
    cursor.execute("DESCRIBE events")
    columns = [column[0] for column in cursor.fetchall()]
    req_columns = {'timestamp', 'latitude', 'longitude', 'event_type'}
    
    print("✅ Conectado a Kafka, esperando eventos...")
    
    # Almacenar eventos en MySQL
    for message in consumer:
        evento = message.value
        print(f"📥 Evento recibido: {evento}")

        try:
            sql = """
                INSERT INTO events (timestamp, latitude, longitude, event_type) 
                VALUES (%s, %s, %s, %s)
            """
            val = (
                evento['timestamp'],
                float(evento['latitude']),
                float(evento['longitude']),
                evento['event_type']
            )
            
            cursor.execute(sql, val)
            conn.commit()
            print("✅ Evento almacenado correctamente")
        except mysql.connector.Error as err:
            print(f"⚠️ Error almacenando evento: {err}")
            conn.rollback()

except Exception as e:
    print(f"⚠️ Error crítico: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("🔌 Conexión a MySQL cerrada")