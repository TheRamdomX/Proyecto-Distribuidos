import re
import json
import time
import threading
from queue import Queue
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException
from kafka import KafkaProducer

# Configuración global
NUM_THREADS = 3
TOPIC_NAME = "waze-events"
KAFKA_SERVER = "kafka:9092"
WINDOW_WIDTH = 16384
WINDOW_HEIGHT = 8064
EVENT_COUNT = 0

# Configuración de cuadrantes
top, bottom = -33.3, -33.7
left, right = -71.0, -70.3
filas, columnas = 60, 60

# Crear Productor Kafka
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
            lat = bottom + i * lat_step + lat_step / 2
            lng = left + j * lng_step + lng_step / 2
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

def extraer_pixeles_translate3d(style_text):
    match = re.search(r'translate3d\(([-\d.]+)px,\s*([-\d.]+)px', style_text)
    if match:
        x = float(match.group(1))
        y = float(match.group(2))
        return x, y
    return None, None

def convertir_pixeles_a_latlon(x, y, lat_center, lng_center):
    lat_deg_per_px = 0.0001
    lng_deg_per_px = 0.0001
    lat = lat_center - (y - WINDOW_HEIGHT/2) * lat_deg_per_px
    lng = lng_center + (x - WINDOW_WIDTH/2) * lng_deg_per_px
    return lat, lng

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
    url = f"https://ul.waze.com/ul?ll={lat_center}%2C{lng_center}&navigate=yes&zoom=16"
    print(f"[Thread-{thread_id}] 🔍 Visitando cuadrante: {lat_center}, {lng_center}")
    driver.get(url)
    time.sleep(2)

    try:
        marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")
        print(f"[Thread-{thread_id}] Se encontraron {len(marcadores)} marcadores")

        for i in range(len(marcadores)):
            try:
                marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")
                marcador = marcadores[i]
                
                style_class = marcador.get_attribute("class")
                style_attr = marcador.get_attribute("style")
                tipo_evento = determinar_tipo_evento(style_class)

                if tipo_evento == "DESCONOCIDO":
                    continue

                x, y = extraer_pixeles_translate3d(style_attr)
                
                if x is not None and y is not None:
                    lat_event, lng_event = convertir_pixeles_a_latlon(x, y, lat_center, lng_center)
                    
                    print(f"[Thread-{thread_id}] 📌 Evento: {tipo_evento} en ({lat_event:.6f}, {lng_event:.6f})")

                    evento = {
                        "timestamp": datetime.now().isoformat(),
                        "latitude": lat_event,
                        "longitude": lng_event,
                        "event_type": tipo_evento
                    }
                    producer.send(TOPIC_NAME, evento)
                    global EVENT_COUNT
                    EVENT_COUNT += 1
                else:
                    print(f"[Thread-{thread_id}] ❌ No se pudo obtener coordenadas para marcador {i}")

            except StaleElementReferenceException:
                print(f"[Thread-{thread_id}] ❌ Error: Elemento obsoleto para marcador {i}")
            except Exception as e:
                print(f"[Thread-{thread_id}] ❌ Error procesando marcador {i}: {e}")

    except Exception as e:
        print(f"[Thread-{thread_id}] ❌ Error procesando cuadrante: {e}")

def worker(q, thread_id):
    driver = iniciar_driver()
    while not q.empty():
        lat, lng = q.get()
        try:
            procesar_cuadrante(driver, lat, lng, thread_id)
        except Exception as e:
            print(f"[Thread-{thread_id}] ❌ Error grave: {e}")
        finally:
            q.task_done()
    driver.quit()

if __name__ == "__main__":
    while True:
        cuadrantes = generar_cuadrantes(top, bottom, left, right, filas, columnas)
        q = Queue()
        for cuadrante in cuadrantes:
            q.put(cuadrante)

        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, args=(q, i+1))
            t.start()
            threads.append(t)

        q.join()
        for t in threads:
            t.join()

        print(f"✅ Iteración de scraping terminada. Total eventos: {EVENT_COUNT}")

        time.sleep(60)  
