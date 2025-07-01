import json
import time
from elasticsearch import Elasticsearch
import redis
from datetime import datetime
from collections import OrderedDict, defaultdict
import threading
import random

# Configuracion
ELASTICSEARCH_URL = "http://elasticsearch:9200"
REDIS_HOST = "redis"
REDIS_PORT = 6379

WAIT_INTERVAL = 5  

# Conexion Elasticsearch
def connect_elasticsearch():
    print(f"🔗 Intentando conectar a Elasticsearch en: {ELASTICSEARCH_URL}")
    try:
        es = Elasticsearch([ELASTICSEARCH_URL])
        print("✅ Cliente de Elasticsearch creado")
        return es
    except Exception as e:
        print(f"❌ Error creando cliente de Elasticsearch: {e}")
        return None

# Esperar a que Elasticsearch esté disponible
def wait_for_elasticsearch_data():
    print("⏳ Esperando a que Elasticsearch esté disponible...")
    while True:
        try:
            es = connect_elasticsearch()
            if es is None:
                print("❌ No se pudo crear el cliente de Elasticsearch")
                time.sleep(WAIT_INTERVAL)
                continue
                
            # Intentar hacer una consulta simple en lugar de ping
            try:
                response = es.search(
                    index="eventos-filtrados",
                    body={
                        "query": {"match_all": {}},
                        "size": 0
                    }
                )
                print("✅ Elasticsearch disponible y respondiendo")
                return
            except Exception as e:
                print(f"⚠️ Elasticsearch no responde a consultas: {e}")
                
        except Exception as e:
            print(f"⚠️ Error conectando a Elasticsearch: {e}")
        
        time.sleep(WAIT_INTERVAL)

# Clase de caché LRU
class LRUCache:
    # Inicializa la caché con una capacidad dada
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()
        self.hits = 0
        self.misses = 0
    
    # Obtiene un elemento de la caché
    def get(self, key):
        if key not in self.cache:
            self.misses += 1
            return None
        self.cache.move_to_end(key)
        self.hits += 1
        return self.cache[key]
    
    # Agrega un elemento a la caché
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
    
    # Obtiene estadísticas de la caché
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

# Clase de caché aleatoria
class RandomCache:
    # Inicializa la caché con una capacidad dada
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = {}
        self.keys = []
        self.hits = 0
        self.misses = 0
    
    # Obtiene un elemento de la caché
    def get(self, key):
        if key not in self.cache:
            self.misses += 1
            return None
        self.hits += 1
        return self.cache[key]
    
    # Agrega un elemento a la caché
    def put(self, key, value):
        if key not in self.cache:
            if len(self.keys) >= self.capacity:
                evicted_key = random.choice(self.keys)
                self.keys.remove(evicted_key)
                del self.cache[evicted_key]
            self.keys.append(key)
        self.cache[key] = value
    
    # Obtiene estadísticas de la caché
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

# Clase del sistema de caché dual
class DualCacheSystem:
    # Inicializa el sistema de caché dual con capacidades para LRU y aleatoria
    def __init__(self, lru_capacity, random_capacity):
        self.redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        self.es = connect_elasticsearch()
        self.lru_cache = LRUCache(lru_capacity)
        self.random_cache = RandomCache(random_capacity)
        self.stats_lock = threading.Lock()
        self.query_distribution = defaultdict(int)
    
    # Procesa una consulta y devuelve el resultado
    def process_query(self, query):
        event_id = query['event_id']
        distribution = query.get('distribution', 'unknown')
        
        with self.stats_lock:
            self.query_distribution[distribution] += 1
        
        lru_result = self.lru_cache.get(event_id)
        random_result = self.random_cache.get(event_id)
        
        # Si no se encuentra en ninguna caché, buscar en Redis
        if not lru_result and not random_result:
            redis_data = self.redis.get(f"event:{event_id}")
            if redis_data:
                data = json.loads(redis_data)
                self.lru_cache.put(event_id, data)
                self.random_cache.put(event_id, data)
                return data
            return None
        
        return lru_result or random_result
    
    # Actualiza la caché con un nuevo evento
    def update_cache(self, event):
        event_id = event['id']
        serialized = json.dumps(event)
        
        self.lru_cache.put(event_id, event)
        self.random_cache.put(event_id, event)
        self.redis.setex(f"event:{event_id}", 3600, serialized)  # Expira en 1 hora
    
    # Busca un evento en Elasticsearch
    def search_in_elasticsearch(self, event_id):
        try:
            response = self.es.search(
                index="eventos-filtrados",
                body={
                    "query": {
                        "term": {
                            "id": event_id
                        }
                    },
                    "size": 1
                }
            )
            
            if response['hits']['total']['value'] > 0:
                event = response['hits']['hits'][0]['_source']
                return event
            return None
        except Exception as e:
            print(f"⚠️ Error buscando en Elasticsearch: {e}")
            return None
    
    # Obtiene estadísticas combinadas de ambas cachés
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

# Función principal: inicia el sistema de caché dual y muestra estadísticas
def run_dual_cache_system():
    print("🔄 Iniciando sistema de caché dual (LRU + Random) con Elasticsearch")
    
    wait_for_elasticsearch_data()
    
    lru_capacity = 5000
    random_capacity = 5000
    cache_system = DualCacheSystem(lru_capacity=lru_capacity, random_capacity=random_capacity)
    
    # Iniciar el hilo para mostrar estadísticas
    def stats_reporter():
        while True:
            time.sleep(5)
            stats = cache_system.get_combined_stats()
            
            print("\n📊 Estadísticas Comparativas de Caché 📊")
            
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
            
    
    threading.Thread(target=stats_reporter, daemon=True).start()
    
    # Simular consultas aleatorias a Elasticsearch
    print("🔄 Iniciando simulación de consultas a Elasticsearch...")
    
    while True:
        try:
            # Obtener un evento aleatorio de Elasticsearch
            response = cache_system.es.search(
                index="eventos-filtrados",
                body={
                    "query": {"match_all": {}},
                    "size": 1,
                    "sort": [{"_script": {"type": "number", "script": {"source": "Math.random()"}, "order": "asc"}}]
                }
            )
            
            if response['hits']['total']['value'] > 0:
                event = response['hits']['hits'][0]['_source']
                event_id = event['id']
                
                # Crear una consulta simulada
                query = {
                    'event_id': event_id,
                    'event_type': event.get('tipo', 'unknown'),  # Usar 'tipo' en lugar de 'type'
                    'distribution': random.choice(['uniform', 'normal', 'exponential'])
                }
                
                print(f"📥 Consulta simulada: evento_id={event_id} tipo={query['event_type']} dist={query['distribution']}")
                
                result = cache_system.process_query(query)

                # Si no se encuentra en ninguna caché, buscar en Redis y Elasticsearch
                if not result:
                    print(f"❌ Cache miss en ambos sistemas para evento {event_id}")
                    
                    redis_key = f"event:{event_id}"
                    redis_data = cache_system.redis.get(redis_key)

                    # Si se encuentra en Redis, actualizar la caché
                    if redis_data:
                        event_data = json.loads(redis_data)
                        cache_system.update_cache(event_data)
                        print(f"✅ Evento {event_id} cacheado desde Redis.")
                    # Si no se encuentra en Redis, buscar en Elasticsearch
                    else:
                        es_event = cache_system.search_in_elasticsearch(event_id)
                        if es_event:
                            cache_system.update_cache(es_event)
                            print(f"✅ Evento {event_id} cacheado desde Elasticsearch.")
                        else:
                            print(f"⚠️ Evento {event_id} no encontrado en Elasticsearch.")
                # Si se encuentra en alguna caché, imprimir el resultado
                else:
                    print(f"✅ Cache hit para evento {event_id}")
            
            # Esperar un tiempo antes de la siguiente consulta
            time.sleep(2)
            
        except Exception as e:
            print(f"⚠️ Error en la simulación: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_dual_cache_system()