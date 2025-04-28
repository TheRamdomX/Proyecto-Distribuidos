# scraper.py (Scraper Waze usando Kafka)

import re
import os
import json
import time
import threading
from datetime import datetime
from queue import Queue
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException
from kafka import KafkaProducer

# Configuración global
NUM_THREADS = 5
TOPIC = "waze-events"
KAFKA_SERVER = "kafka:9092"

# División de cuadrantes
TOP, BOTTOM = -33.3, -33.7
LEFT, RIGHT = -71.0, -70.3
FILAS, COLUMNAS = 50, 50

# Iniciar Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generar cuadrantes
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

# Iniciar driver Selenium
def iniciar_driver():
    opciones = Options()
    opciones.add_argument("--headless=new")
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--no-sandbox")
    opciones.add_argument("--disable-dev-shm-usage")
    opciones.add_argument("--window-size=16384,8064")
    servicio = Service()
    return webdriver.Chrome(service=servicio, options=opciones)

# Determinar tipo de evento
def determinar_tipo_evento(style):
    match style:
        case style if "wm-alert-icon--road-closed" in style:
            return "CAMINO CORTADO"
        case style if "wm-alert-icon--hazard" in style:
            return "PELIGRO"
        case style if "wm-alert-icon--police" in style:
            return "POLICIA"
        case style if "wm-alert-icon--accident" in style:
            return "ACCIDENTE"
        case style if "waze_jam" in style:
            return "CONGESTION"
        case _:
            return "DESCONOCIDO"

# Extraer pixeles

def extraer_pixeles_translate3d(style_text):
    match = re.search(r'translate3d\(([-\d.]+)px,\s*([-\d.]+)px', style_text)
    if match:
        x = float(match.group(1))
        y = float(match.group(2))
        return x, y
    else:
        return None, None

# Convertir pixeles a latlon
def convertir_pixeles_a_latlon(x, y, width, height, lat_min, lat_max, lon_min, lon_max):
    lon = lon_min + (lon_max - lon_min) * (x / width)
    lat = lat_max - (lat_max - lat_min) * (y / height)
    return lat, lon

# Procesar un cuadrante
def procesar_cuadrante(driver, lat, lng, thread_id):
    url = f"https://ul.waze.com/ul?ll={lat}%2C{lng}&navigate=yes&zoom=16"
    print(f"[Thread-{thread_id}] Visitando cuadrante: {lat}, {lng}")
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
                    lat_event, lon_event = convertir_pixeles_a_latlon(
                        x, y, 16384, 8064, lat, lat - 0.1, lng - 0.1, lng + 0.1
                    )

                    evento = {
                        "timestamp": datetime.now().isoformat(),
                        "latitude": lat_event,
                        "longitude": lon_event,
                        "event_type": tipo_evento
                    }
                    producer.send(TOPIC, evento)
                    print(f"[Thread-{thread_id}] Evento enviado a Kafka: {evento}")

            except StaleElementReferenceException:
                print(f"[Thread-{thread_id}] ❌ Error: Stale element reference para marcador {i}")
    except Exception as e:
        print(f"[Thread-{thread_id}] ❌ Error procesando cuadrante: {e}")

# Worker

def worker(q, thread_id):
    print(f"[Thread-{thread_id}] Iniciando")
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
    print(f"[Thread-{thread_id}] Finalizado")

# Main

if __name__ == "__main__":
    cuadrantes = generar_cuadrantes(TOP, BOTTOM, LEFT, RIGHT, FILAS, COLUMNAS)

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

    producer.flush()
    print("✅ Todos los cuadrantes procesados. Eventos enviados a Kafka.")