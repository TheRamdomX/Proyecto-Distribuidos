import json
import time
import os
import sys
from kafka import KafkaConsumer
import redis
from datetime import datetime
from collections import OrderedDict
import threading
import mysql.connector

# Configuración
KAFKA_SERVER = "kafka:9092"
QUERY_TOPIC = "traffic-queries"
CACHE_TOPIC = "cache-updates"
REDIS_HOST = "redis"
REDIS_PORT = 6379

MYSQL_HOST = "mysql"
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "waze_db"
WAIT_INTERVAL = 5  
MIN_EVENTS = 2000   

def connect_mysql():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

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
                print(f"✅ Datos iniciales cargados ({count} eventos encontrados)")
                return
            else:
                print(f"🕒 Esperando más datos... ({count}/{MIN_EVENTS} eventos)")
                time.sleep(WAIT_INTERVAL)
        except Exception as e:
            print(f"⚠️ Error verificando datos iniciales: {e}")
            time.sleep(WAIT_INTERVAL)

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()
    
    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]
    
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

class LFUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = {}
        self.freq = {}
        self.min_freq = 0
    
    def get(self, key):
        if key not in self.cache:
            return None
        
        self.freq[key] += 1
        return self.cache[key]
    
    def put(self, key, value):
        if self.capacity == 0:
            return
        
        if key in self.cache:
            self.cache[key] = value
            self.freq[key] += 1
            return
        
        if len(self.cache) >= self.capacity:
            min_key = min(self.freq, key=lambda k: self.freq[k])
            del self.cache[min_key]
            del self.freq[min_key]
        
        self.cache[key] = value
        self.freq[key] = 1
        self.min_freq = 1

class CacheSystem:
    def __init__(self, policy='LRU', capacity=1000):
        self.redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        self.policy = policy
        self.capacity = capacity
        
        if policy == 'LRU':
            self.cache = LRUCache(capacity)
        elif policy == 'LFU':
            self.cache = LFUCache(capacity)
        else:
            raise ValueError("Política de caché no soportada")
        
        self.hits = 0
        self.misses = 0
        self.stats_lock = threading.Lock()
    
    def process_query(self, query):
        event_id = query['event_id']
        
        cached = self.cache.get(event_id)
        if cached:
            with self.stats_lock:
                self.hits += 1
            return cached
        
        redis_data = self.redis.get(f"event:{event_id}")
        if redis_data:
            data = json.loads(redis_data)
            self.cache.put(event_id, data)
            with self.stats_lock:
                self.hits += 1
            return data
        
        # 3. Cache miss
        with self.stats_lock:
            self.misses += 1
        return None
    
    def update_cache(self, event):
        event_id = event['id']
        serialized = json.dumps(event)
        
        self.cache.put(event_id, event)
        self.redis.setex(f"event:{event_id}", 3600, serialized) 
    
    def get_cache_size(self):
        """Obtiene el número de elementos actualmente en la caché local"""
        if hasattr(self.cache, 'cache'):
            return len(self.cache.cache)
        return 0

    def get_stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total) * 100 if total > 0 else 0
        current_cache_size = self.get_cache_size()
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
            "policy": self.policy,
            "capacity": self.capacity,
            "current_size": current_cache_size
        }


def run_cache_system(policy='LRU', capacity=1000):
    print(f"🔄 Iniciando sistema de caché ({policy}, capacidad: {capacity})")
    
    wait_for_initial_data()
    
    consumer = KafkaConsumer(
        QUERY_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='cache-group'
    )
    
    cache = CacheSystem(policy=policy, capacity=capacity)
    
    def stats_reporter():
        while True:
            time.sleep(30)
            stats = cache.get_stats()
            print(f"\n📊 Estadísticas de Caché ({policy}):")
            print(f"• Hit Rate: {stats['hit_rate']:.2f}%")
            print(f"• Hits: {stats['hits']} | Misses: {stats['misses']}")
            print(f"• Uso de Caché: {stats['current_size']}/{stats['capacity']} elementos")

    
    threading.Thread(target=stats_reporter, daemon=True).start()
    
    for message in consumer:
        query = message.value
        print(f"📥 Consulta recibida: evento_id={query['event_id']} tipo={query['event_type']}")
        result = cache.process_query(query)

        if not result:
            print(f"❌ Cache miss para evento {query['event_id']} — Buscando en Redis/MySQL...")

            redis_key = f"event:{query['event_id']}"
            redis_data = cache.redis.get(redis_key)

            if redis_data:
                event = json.loads(redis_data)
                cache.update_cache(event)
                print(f"✅ Evento {query['event_id']} cacheado desde Redis.")
            else:
                try:
                    conn = connect_mysql()
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT * FROM events WHERE id = %s", (query['event_id'],))
                    event = cursor.fetchone()
                    cursor.close()
                    conn.close()

                    if event:
                        if isinstance(event['timestamp'], datetime):
                            event['timestamp'] = event['timestamp'].isoformat()

                        cache.update_cache(event)
                        print(f"✅ Evento {query['event_id']} cacheado desde MySQL.")
                    else:
                        print(f"⚠️ Evento {query['event_id']} no encontrado en MySQL.")

                except Exception as e:
                    print(f"⚠️ Error buscando evento {query['event_id']} en MySQL: {e}")
        else:
            print(f"✅ Cache hit para evento {query['event_id']}")



if __name__ == "__main__":
    policy = os.getenv('CACHE_POLICY', 'LRU')
    try:
        capacity = int(os.getenv('CACHE_CAPACITY', '1000'))
    except ValueError:
        capacity = 1000
    
    run_cache_system(policy=policy, capacity=capacity)