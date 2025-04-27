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

# Configuración global
NUM_THREADS = 4  # Número de hilos concurrentes (ajustar según recursos)

# División de cuadrantes (igual que antes)
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

# Configuración de Selenium (ahora por thread)
def iniciar_driver():
    opciones = Options()
    opciones.add_argument("--headless=new")
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--no-sandbox")
    opciones.add_argument("--disable-dev-shm-usage")
    opciones.add_argument("--window-size=16384,8064")
    servicio = Service()
    return webdriver.Chrome(service=servicio, options=opciones)

# Función para procesar un cuadrante
def procesar_cuadrante(driver, lat, lng, thread_id):
    url = f"https://ul.waze.com/ul?ll={lat}%2C{lng}&navigate=yes&zoom=16"
    print(f"\n[Thread-{thread_id}] 🔍 Visitando cuadrante: {lat}, {lng}")
    driver.get(url)
    time.sleep(2)

    try:
        marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")
        print(f"[Thread-{thread_id}] Se encontraron {len(marcadores)} marcadores")

        mapa = driver.find_element(By.ID, "map")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "map")))

        mapa_width = mapa.size['width']
        mapa_height = mapa.size['height']
        print(f"[Thread-{thread_id}] Mapa dimensions: width={mapa_width}, height={mapa_height}")

        for i in range(len(marcadores)):
            try:
                marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")
                marcador = marcadores[i]
                style = marcador.get_attribute("style")
                print(f"[Thread-{thread_id}] Marcador {i}: {style}")
            except StaleElementReferenceException:
                print(f"[Thread-{thread_id}] ❌ Error: Stale element reference for marcador {i}")
    except Exception as e:
        print(f"[Thread-{thread_id}] ❌ Error while processing cuadrante: {e}")

# Función worker para los threads
def worker(q, thread_id):
    print(f"[Thread-{thread_id}] Iniciando worker")
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
    print(f"[Thread-{thread_id}] Finalizando worker")

# Main
if __name__ == "__main__":
    top, bottom = -33.3, -33.7
    left, right = -71.0, -70.3
    filas, columnas = 4, 5

    cuadrantes = generar_cuadrantes(top, bottom, left, right, filas, columnas)
    
    # Crear cola de trabajo
    q = Queue()
    for cuadrante in cuadrantes:
        q.put(cuadrante)

    # Crear e iniciar threads
    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(q, i+1))
        t.start()
        threads.append(t)

    # Esperar a que todos los trabajos terminen
    q.join()

    # Esperar a que todos los threads finalicen
    for t in threads:
        t.join()

    print("✅ Todos los cuadrantes han sido procesados")