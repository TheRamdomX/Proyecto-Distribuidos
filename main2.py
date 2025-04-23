from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time
import re

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
    time.sleep(5)

    marcadores = driver.find_elements(By.CSS_SELECTOR, "div.leaflet-marker-icon")
    print(f"Se encontraron {len(marcadores)} marcadores")

    mapa = driver.find_element(By.ID, "map")

    for i, marcador in enumerate(marcadores):
        style = marcador.get_attribute("style")
        match = re.search(r'translate3d\(([\d\.]+)px,\s*([\d\.]+)px', style)
        if match:
            x = float(match.group(1))
            y = float(match.group(2))

            # Sólo si están dentro del viewport (1920x1080)
            if x < 1920 and y < 1080:
                print(f"[{i}] Clic en x={x}, y={y}")
                actions = ActionChains(driver)
                actions.move_to_element_with_offset(mapa, x, y).click().perform()
                time.sleep(0.5)
            else:
                print(f"[{i}] Fuera de viewport: x={x}, y={y}")

# Configuración de Selenium
def iniciar_driver():
    opciones = Options()
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--window-size=1920,1080")
    # NO headless para que puedas verlo
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
