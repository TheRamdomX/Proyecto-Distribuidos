import re
import os
import csv
import time
import threading
from datetime import datetime
from queue import Queue
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException


# Configuración global
NUM_THREADS = 5  # Número de hilos concurrentes (ajustar según recursos)
CSV_FILENAME = "waze_eventos3.csv"
CSV_HEADERS = ["timestamp", "latitud", "longitud", "tipo_evento"]

# División de cuadrantes
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

# Configuración de Selenium (por thread)
def iniciar_driver():
    opciones = Options()
    opciones.add_argument("--headless=new")
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--no-sandbox")
    opciones.add_argument("--disable-dev-shm-usage")
    opciones.add_argument("--window-size=16384,8064")
    servicio = Service()
    return webdriver.Chrome(service=servicio, options=opciones)

# Función para escribir en el CSV (con bloqueo de thread)
def guardar_evento(evento, lock):
    file_exists = os.path.isfile(CSV_FILENAME)
    
    with lock:
        with open(CSV_FILENAME, mode='a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(evento)

# Función para determinar el tipo de evento basado en el estilo del marcador
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

# Función para extraer las coordenadas x, y del estilo transform: translate3d(x, y, 0)
def extraer_pixeles_translate3d(style_text):
    match = re.search(r'translate3d\(([-\d.]+)px,\s*([-\d.]+)px', style_text)
    if match:
        x = float(match.group(1))
        y = float(match.group(2))
        return x, y
    else:
        return None, None

# Función para convertir los píxeles a latitud y longitud
def convertir_pixeles_a_latlon(x, y, width, height, lat_min, lat_max, lon_min, lon_max):
    lon = lon_min + (lon_max - lon_min) * (x / width)
    lat = lat_max - (lat_max - lat_min) * (y / height)
    return lat, lon

# Función para procesar un cuadrante
def procesar_cuadrante(driver, lat, lng, thread_id, lock):
    url = f"https://ul.waze.com/ul?ll={lat}%2C{lng}&navigate=yes&zoom=16"
    print(f"\n[Thread-{thread_id}] 🔍 Visitando cuadrante: {lat}, {lng}")
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
                    continue  # Ignorar marcadores desconocidos

                # Extraer x, y en píxeles
                x, y = extraer_pixeles_translate3d(style_attr)
                
                if x is not None and y is not None:
                    lat_event, lon_event = convertir_pixeles_a_latlon(
                        x, y, 16384, 8064,  # Tamaño de la ventana del navegador en píxeles
                        lat, lat - 0.1, lng - 0.1, lng + 0.1  # Establecer un pequeño rango para el cuadrante
                    )
                    
                    print(f"[Thread-{thread_id}] Marcador {i}: {tipo_evento} en ({lat_event}, {lon_event})")

                    evento = {
                        "timestamp": datetime.now().isoformat(),
                        "latitud": lat_event,
                        "longitud": lon_event,
                        "tipo_evento": tipo_evento
                    }
                    guardar_evento(evento, lock)
                else:
                    print(f"[Thread-{thread_id}] ❌ No se pudo parsear coordenadas para marcador {i}")

            except StaleElementReferenceException:
                print(f"[Thread-{thread_id}] ❌ Error: Stale element reference for marcador {i}")
    except Exception as e:
        print(f"[Thread-{thread_id}] ❌ Error while processing cuadrante: {e}")

# Función worker para los threads
def worker(q, thread_id, lock):
    print(f"[Thread-{thread_id}] Iniciando worker")
    driver = iniciar_driver()
    
    while not q.empty():
        lat, lng = q.get()
        try:
            procesar_cuadrante(driver, lat, lng, thread_id, lock)
        except Exception as e:
            print(f"[Thread-{thread_id}] ❌ Error grave: {e}")
        finally:
            q.task_done()
    
    driver.quit()
    print(f"[Thread-{thread_id}] Finalizando worker")

# Main
if __name__ == "__main__":
    top, bottom = -33.3, -33.7
    left, right = -71.0, -70.3
    filas, columnas = 30, 30

    cuadrantes = generar_cuadrantes(top, bottom, left, right, filas, columnas)
    
    # Crear cola de trabajo
    q = Queue()
    for cuadrante in cuadrantes:
        q.put(cuadrante)

    # Crear lock para acceso seguro al CSV
    lock = threading.Lock()

    # Crear e iniciar threads
    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(q, i+1, lock))
        t.start()
        threads.append(t)

    # Esperar a que todos los trabajos terminen
    q.join()

    # Esperar a que todos los threads finalicen
    for t in threads:
        t.join()

    print("✅ Todos los cuadrantes han sido procesados")
    print(f"📊 Los resultados se han guardado en {CSV_FILENAME}")
