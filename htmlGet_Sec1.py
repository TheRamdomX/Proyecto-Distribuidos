from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import shutil
import time
import uuid
import os

def viewHTML(url):
    opciones = Options()
    opciones.add_argument("--headless=new")
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--no-sandbox")
    opciones.add_argument("--disable-dev-shm-usage")
    opciones.add_argument("--window-size=16384,8064")

    temp_profile_dir = f"/tmp/chrome_profile_{uuid.uuid4()}"
    os.makedirs(temp_profile_dir, exist_ok=True)
    opciones.add_argument(f"--user-data-dir={temp_profile_dir}")

    driver = webdriver.Chrome(service=Service(), options=opciones)

    try:
        ancho = driver.execute_script("return window.innerWidth")
        alto = driver.execute_script("return window.innerHeight")
        print(f"Tamaño del viewport: {ancho}x{alto}")

        driver.get(url)
        time.sleep(5)

        html = driver.page_source

        with open("Sec1.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("HTML Sec1 guardado")

        return html
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        driver.quit()
        shutil.rmtree(temp_profile_dir)

url = "https://ul.waze.com/ul?ll=-33.4368613710077%2C-70.63374640102157&navigate=yes&zoom=16&utm_campaign=default&utm_source=waze_website&utm_medium=lm_share_location"
viewHTML(url)
