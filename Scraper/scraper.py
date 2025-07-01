import re
import json
import time
import math
import threading
from queue import Queue
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException
from kafka import KafkaProducer
import geopandas as gpd
from shapely.geometry import Point

comunas_rm = gpd.read_file("RM.geojson")
comunas_rm = comunas_rm.to_crs(epsg=4326)

def get_comuna_from_coords(lat, lon):
    point = Point(lon, lat)
    for idx, row in comunas_rm.iterrows():
        if row['geometry'].contains(point):
            return row['Comuna']
    return None

# Configuracion
NUM_THREADS = 15
TOPIC_NAME = "waze-events"
KAFKA_SERVER = "kafka:9092"
WINDOW_WIDTH = 1024
WINDOW_HEIGHT = 768
ZOOM_LEVEL = 16
EVENT_COUNT = 0

# Coordenadas y cuadrantes del scrapeo
top, bottom = -33.2, -33.7
left, right = -70.8, -70.3
filas, columnas = 45, 45 # 2025 cuadrantes de 500m x 500m

# Configuracion del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generar_cuadrantes(top, bottom, left, right, filas, columnas):
    lat_step = (top - bottom) / filas
    lng_step = (right - left) / columnas
    cuadrantes = []
    
    for i in range(filas):
        for j in range(columnas):
            j_actual = j if i % 2 == 0 else columnas - 1 - j
            lat = bottom + i * lat_step + lat_step / 2
            lng = left + j_actual * lng_step + lng_step / 2
            cuadrantes.append((lat, lng))
    
    return cuadrantes

def iniciar_driver():
    opciones = Options()
    opciones.add_argument("--headless=new")
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--no-sandbox")
    opciones.add_argument("--disable-dev-shm-usage")
    opciones.add_argument(f"--window-size={WINDOW_WIDTH},{WINDOW_HEIGHT}")
    servicio = Service()
    return webdriver.Chrome(service=servicio, options=opciones)

def extraer_pixeles(style_text):
    try:
        # Buscar el patrón translate3d(Xpx, Ypx)
        match = re.search(r'translate3d\(([-\d.]+)px,\s*([-\d.]+)px', style_text)
        if match:
            x = float(match.group(1))
            y = float(match.group(2))
            return x, y
        return None, None
    except Exception as e:
        print(f"Error en extraer_pixeles: {e}")
        return None, None

def convertir_pixeles_web_mercator(x, y, lat_center, lng_center, zoom, window_width, window_height):
    tile_size = 256
    scale = 2 ** zoom
    world_size = tile_size * scale

    def latlng_to_pixels(lat, lng):
        x = (lng + 180) / 360 * world_size
        sin_lat = math.sin(math.radians(lat))
        y = (0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)) * world_size
        return x, y

    def pixels_to_latlng(x, y):
        lng = x / world_size * 360 - 180
        n = math.pi - 2 * math.pi * y / world_size
        lat = math.degrees(math.atan(math.sinh(n)))
        return lat, lng

    center_x, center_y = latlng_to_pixels(lat_center, lng_center)
    dx = x - window_width / 2
    dy = y - window_height / 2
    point_x = center_x + dx
    point_y = center_y + dy
    return pixels_to_latlng(point_x, point_y)

def determinar_tipo_evento(style):
    if "wm-alert-icon--road-closed" in style:
        return "CAMINO CORTADO"
    if "wm-alert-icon--hazard" in style:
        return "PELIGRO"
    if "wm-alert-icon--police" in style:
        return "POLICIA"
    if "wm-alert-icon--accident" in style:
        return "ACCIDENTE"
    if "waze_jam" in style:
        return "CONGESTION"
    return "DESCONOCIDO"

def procesar_cuadrante(driver, lat_center, lng_center, thread_id):
    url = f"https://ul.waze.com/ul?ll={lat_center}%2C{lng_center}&navigate=yes&zoom={ZOOM_LEVEL}"
    print(f"[Thread-{thread_id}] Visitando cuadrante: {lat_center}, {lng_center}")

    try:
        driver.get(url)
        time.sleep(4)

        marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")

        for i in range(len(marcadores)):
            try:
                marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")
                if i >= len(marcadores):
                    break

                marcador = marcadores[i]
                style_class = marcador.get_attribute("class")
                style_attr = marcador.get_attribute("style")
                tipo_evento = determinar_tipo_evento(style_class)

                if tipo_evento == "DESCONOCIDO":
                    continue

                x, y = extraer_pixeles(style_attr)
                if x is not None and y is not None:
                    lat_event, lng_event = convertir_pixeles_web_mercator(
                        x, y, lat_center, lng_center, ZOOM_LEVEL, WINDOW_WIDTH, WINDOW_HEIGHT)
                    try:
                        comuna = get_comuna_from_coords(lat_event, lng_event)
                        if comuna is None:
                            comuna = "Desconocida"
                    except Exception as e:
                        print(f"[Thread-{thread_id}] Error obteniendo comuna: {e}")
                        comuna = "Desconocida"

                    print(f"[Thread-{thread_id}] Evento: {tipo_evento} en ({lat_event:.6f}, {lng_event:.6f}) - Comuna: {comuna} - Píxeles: ({x:.1f}, {y:.1f})")

                    evento = {
                        "timestamp": datetime.now().isoformat(),
                        "latitude": lat_event,
                        "longitude": lng_event,
                        "event_type": tipo_evento,
                        "comuna": comuna
                    }
                    producer.send(TOPIC_NAME, evento)
                    global EVENT_COUNT
                    EVENT_COUNT += 1
                else:
                    print(f"[Thread-{thread_id}] No se pudo obtener coordenadas para marcador {i}")

            except StaleElementReferenceException:
                print(f"[Thread-{thread_id}] Elemento obsoleto para marcador {i}")
                time.sleep(1)
            except Exception as e:
                print(f"[Thread-{thread_id}] Error procesando marcador {i}: {e}")
                time.sleep(1)

    except Exception as e:
        print(f"[Thread-{thread_id}] Error procesando cuadrante: {e}")
        raise

def worker(q, thread_id, processed_quadrants):
    max_retries = 3
    retry_delay = 5

    while not q.empty():
        lat, lng = q.get()
        cuadrante = (lat, lng)
        
        # Verificar si el cuadrante ya fue procesado
        if cuadrante in processed_quadrants:
            q.task_done()
            continue
            
        retries = 0
        driver = None

        while retries < max_retries:
            try:
                if driver is None:
                    driver = iniciar_driver()
                procesar_cuadrante(driver, lat, lng, thread_id)
                # Marcar el cuadrante como procesado
                processed_quadrants.add(cuadrante)
                break
            except Exception as e:
                print(f"[Thread-{thread_id}] Error grave: {e}")
                retries += 1
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = None
                if retries < max_retries:
                    print(f"[Thread-{thread_id}] Reintentando en {retry_delay} segundos... (Intento {retries + 1}/{max_retries})")
                    time.sleep(retry_delay)
            finally:
                q.task_done()

        if driver:
            try:
                driver.quit()
            except:
                pass

if __name__ == "__main__":
    while True:
        cuadrantes = generar_cuadrantes(top, bottom, left, right, filas, columnas)
        q = Queue()
        for cuadrante in cuadrantes:
            q.put(cuadrante)

        # Conjunto para rastrear cuadrantes procesados
        processed_quadrants = set()
        
        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, args=(q, i+1, processed_quadrants))
            t.start()
            threads.append(t)

        q.join()
        for t in threads:
            t.join()

        print(f"Iteración de scraping terminada. Total eventos: {EVENT_COUNT}")
        time.sleep(600)
