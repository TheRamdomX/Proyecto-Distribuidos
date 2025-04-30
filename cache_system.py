import json
import time
import os
import sys
from kafka import KafkaConsumer
import redis
from datetime import datetime
from collections import OrderedDict, defaultdict
import threading
import mysql.connector
import random

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
MIN_EVENTS = 10000   

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
        self.hits = 0
        self.misses = 0
    
    def get(self, key):
        if key not in self.cache:
            self.misses += 1
            return None
        self.cache.move_to_end(key)
        self.hits += 1
        return self.cache[key]
    
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
    
    def get_stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total) * 100 if total > 0 else 0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
            "policy": "LRU",
            "capacity": self.capacity,
            "current_size": len(self.cache)
        }

class RandomCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = {}
        self.keys = []
        self.hits = 0
        self.misses = 0
    
    def get(self, key):
        if key not in self.cache:
            self.misses += 1
            return None
        self.hits += 1
        return self.cache[key]
    
    def put(self, key, value):
        if key not in self.cache:
            if len(self.keys) >= self.capacity:
                evicted_key = random.choice(self.keys)
                self.keys.remove(evicted_key)
                del self.cache[evicted_key]
            self.keys.append(key)
        self.cache[key] = value
    
    def get_stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total) * 100 if total > 0 else 0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
            "policy": "Random",
            "capacity": self.capacity,
            "current_size": len(self.cache)
        }

class DualCacheSystem:
    def __init__(self, lru_capacity=5000, random_capacity=5000):
        self.redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        self.lru_cache = LRUCache(lru_capacity)
        self.random_cache = RandomCache(random_capacity)
        self.stats_lock = threading.Lock()
        self.query_distribution = defaultdict(int)
    
    def process_query(self, query):
        event_id = query['event_id']
        distribution = query.get('distribution', 'unknown')
        
        with self.stats_lock:
            self.query_distribution[distribution] += 1
        
        lru_result = self.lru_cache.get(event_id)
        random_result = self.random_cache.get(event_id)
        
        if not lru_result and not random_result:
            redis_data = self.redis.get(f"event:{event_id}")
            if redis_data:
                data = json.loads(redis_data)
                self.lru_cache.put(event_id, data)
                self.random_cache.put(event_id, data)
                return data
            return None
        
        return lru_result or random_result
    
    def update_cache(self, event):
        event_id = event['id']
        serialized = json.dumps(event)
        
        self.lru_cache.put(event_id, event)
        self.random_cache.put(event_id, event)
        self.redis.setex(f"event:{event_id}", 3600, serialized)  # Expira en 1 hora
    
    def get_combined_stats(self):
        lru_stats = self.lru_cache.get_stats()
        random_stats = self.random_cache.get_stats()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "lru_cache": lru_stats,
            "random_cache": random_stats,
            "query_distribution": dict(self.query_distribution),
            "redis_keys": self.redis.dbsize()
        }

def run_dual_cache_system():
    print("🔄 Iniciando sistema de caché dual (LRU + Random)")
    
    wait_for_initial_data()
    
    consumer = KafkaConsumer(
        QUERY_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='cache-group'
    )
    
    lru_capacity = 5000
    random_capacity = 5000
    cache_system = DualCacheSystem(lru_capacity=lru_capacity, random_capacity=random_capacity)
    
    def stats_reporter():
        while True:
            time.sleep(5)
            stats = cache_system.get_combined_stats()
            
            print("\n📊📊 Estadísticas Comparativas de Caché 📊📊")
            print(f"⏰ Última actualización: {stats['timestamp']}")
            print(f"🔑 Claves en Redis: {stats['redis_keys']}")
            
            print("\n🔵 LRU Cache:")
            lru = stats['lru_cache']
            print(f"• Hit Rate: {lru['hit_rate']:.2f}%")
            print(f"• Hits: {lru['hits']} | Misses: {lru['misses']}")
            print(f"• Uso: {lru['current_size']}/{lru['capacity']} elementos")
            
            print("\n🔴 Random Cache:")
            rand = stats['random_cache']
            print(f"• Hit Rate: {rand['hit_rate']:.2f}%")
            print(f"• Hits: {rand['hits']} | Misses: {rand['misses']}")
            print(f"• Uso: {rand['current_size']}/{rand['capacity']} elementos")
            
            print("\n📈 Distribución de Consultas:")
            for dist, count in stats['query_distribution'].items():
                print(f"• {dist}: {count} consultas")
    
    threading.Thread(target=stats_reporter, daemon=True).start()
    
    for message in consumer:
        query = message.value
        print(f"📥 Consulta recibida: evento_id={query['event_id']} tipo={query['event_type']} dist={query.get('distribution', 'N/A')}")
        
        result = cache_system.process_query(query)

        if not result:
            print(f"❌ Cache miss en ambos sistemas para evento {query['event_id']}")
            
            redis_key = f"event:{query['event_id']}"
            redis_data = cache_system.redis.get(redis_key)

            if redis_data:
                event = json.loads(redis_data)
                cache_system.update_cache(event)
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

                        cache_system.update_cache(event)
                        print(f"✅ Evento {query['event_id']} cacheado desde MySQL.")
                    else:
                        print(f"⚠️ Evento {query['event_id']} no encontrado en MySQL.")

                except Exception as e:
                    print(f"⚠️ Error buscando evento {query['event_id']} en MySQL: {e}")
        else:
            print(f"✅ Cache hit para evento {query['event_id']}")

if __name__ == "__main__":
    run_dual_cache_system()