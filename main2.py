from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import time

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

# Simulación de clicks en eventos
def procesar_cuadrante(driver, lat, lng):
    url = f"https://ul.waze.com/ul?ll={lat}%2C{lng}&navigate=yes&zoom=16"
    print(f"\n[🔍] Visitando cuadrante: {lat}, {lng}")
    driver.get(url)
    time.sleep(2)

    try:
        marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")
        print(f"Se encontraron {len(marcadores)} marcadores")

        mapa = driver.find_element(By.ID, "map")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "map")))

        mapa_width = mapa.size['width']
        mapa_height = mapa.size['height']
        print(f"Mapa dimensions: width={mapa_width}, height={mapa_height}")

        for i in range(len(marcadores)):
            try:
                # Re-locate the markers dynamically
                marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")
                marcador = marcadores[i]
                style = marcador.get_attribute("style")
                print(f"Marcador {i}: {style}")
            except StaleElementReferenceException:
                print(f"[❌] Error: Stale element reference for marcador {i}")
    except Exception as e:
        print(f"[❌] Error while processing cuadrante: {e}")

# Configuración de Selenium
def iniciar_driver():
    opciones = Options()
    opciones.add_argument("--headless=new")
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--no-sandbox")
    opciones.add_argument("--disable-dev-shm-usage")
    opciones.add_argument("--window-size=16384,8064")
    servicio = Service()
    return webdriver.Chrome(service=servicio, options=opciones)

# Main
if __name__ == "__main__":
    top, bottom = -33.3, -33.7
    left, right = -71.0, -70.3
    filas, columnas = 4, 5

    cuadrantes = generar_cuadrantes(top, bottom, left, right, filas, columnas)
    driver = iniciar_driver()

    try:
        for lat, lng in cuadrantes:
            procesar_cuadrante(driver, lat, lng)
            time.sleep(2)
    except Exception as e:
        print(f"[❌] Error: {e}")
    finally:
        driver.quit()
