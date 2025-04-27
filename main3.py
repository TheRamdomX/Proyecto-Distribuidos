from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import time
import threading
from queue import Queue
import csv
from datetime import datetime
import os

# Configuración global
NUM_THREADS = 4  # Número de hilos concurrentes (ajustar según recursos)
CSV_FILENAME = "waze_eventos.csv"
CSV_HEADERS = ["timestamp", "thread_id", "latitud", "longitud", "tipo_evento", "estilo_marcador", "url_cuadrante"]

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
    if "waze_construction" in style:
        return "OBRA"
    elif "waze_hazard" in style:
        return "PELIGRO"
    elif "waze_police" in style:
        return "POLICIA"
    elif "waze_crash" in style:
        return "ACCIDENTE"
    elif "waze_jam" in style:
        return "CONGESTION"
    else:
        return "DESCONOCIDO"

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
                style = marcador.get_attribute("style")
                tipo_evento = determinar_tipo_evento(style)
                
                print(f"[Thread-{thread_id}] Marcador {i}: {tipo_evento} - {style}")
                
                # Guardar evento en CSV
                evento = {
                    "timestamp": datetime.now().isoformat(),
                    "thread_id": thread_id,
                    "latitud": lat,
                    "longitud": lng,
                    "tipo_evento": tipo_evento,
                    "estilo_marcador": style,
                    "url_cuadrante": url
                }
                guardar_evento(evento, lock)
                
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