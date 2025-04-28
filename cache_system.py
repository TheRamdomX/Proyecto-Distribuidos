import json
import time
import os
from kafka import KafkaConsumer
import redis
from collections import OrderedDict
import threading

# Configuración
KAFKA_SERVER = "kafka:9092"
QUERY_TOPIC = "traffic-queries"
CACHE_TOPIC = "cache-updates"
REDIS_HOST = "redis"
REDIS_PORT = 6379

# Políticas de caché
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
        
        # Actualizar frecuencia
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
            # Encontrar clave con menor frecuencia
            min_key = min(self.freq, key=lambda k: self.freq[k])
            del self.cache[min_key]
            del self.freq[min_key]
        
        self.cache[key] = value
        self.freq[key] = 1
        self.min_freq = 1

# Sistema de Caché
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
        
        # 1. Verificar en caché local
        cached = self.cache.get(event_id)
        if cached:
            with self.stats_lock:
                self.hits += 1
            return cached
        
        # 2. Verificar en Redis
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
        
        # Actualizar ambas capas de caché
        self.cache.put(event_id, event)
        self.redis.setex(f"event:{event_id}", 3600, serialized)  # Expira en 1 hora
    
    def get_stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total) * 100 if total > 0 else 0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
            "policy": self.policy,
            "capacity": self.capacity
        }

def run_cache_system(policy='LRU', capacity=1000):
    print(f"🔄 Iniciando sistema de caché ({policy}, capacidad: {capacity})")
    
    # Configurar consumidor Kafka
    consumer = KafkaConsumer(
        QUERY_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='cache-group'
    )
    
    # Inicializar caché
    cache = CacheSystem(policy=policy, capacity=capacity)
    
    # Hilo para mostrar estadísticas periódicamente
    def stats_reporter():
        while True:
            time.sleep(30)
            stats = cache.get_stats()
            print(f"\n📊 Estadísticas de Caché ({policy}):")
            print(f"• Hit Rate: {stats['hit_rate']:.2f}%")
            print(f"• Hits: {stats['hits']} | Misses: {stats['misses']}")
    
    threading.Thread(target=stats_reporter, daemon=True).start()
    
    # Procesar consultas
    for message in consumer:
        query = message.value
        result = cache.process_query(query)
        
        if not result:
            print(f"❌ Cache miss para evento {query['event_id']}")
        else:
            print(f"✅ Cache hit para evento {query['event_id']}")

if __name__ == "__main__":
    
    policy = os.getenv('CACHE_POLICY', 'LRU')
    try:
        capacity = int(os.getenv('CACHE_CAPACITY', '1000'))
    except ValueError:
        capacity = 1000
    
    run_cache_system(policy=policy, capacity=capacity)