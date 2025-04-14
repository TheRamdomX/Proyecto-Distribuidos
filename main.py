from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time

def descargar_html_con_selenium(url, tiempo_espera=5):
    opciones = Options()
    opciones.add_argument("--headless=new")
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--window-size=8192,4128")  

    driver = webdriver.Chrome(service=Service()  , options=opciones)
    
    try:
        driver.maximize_window()
        ancho = driver.execute_script("return window.innerWidth")
        alto = driver.execute_script("return window.innerHeight")
        print(f"Tamaño del viewport: {ancho}x{alto}")

        driver.get(url)
        time.sleep(tiempo_espera)

        html = driver.page_source

        with open("waze.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("HTML guardado como 'waze.html'")

        return html
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        driver.quit()


url = "https://ul.waze.com/ul?ll=-33.4368613710077%2C-70.63374640102157&navigate=yes&zoom=16&utm_campaign=default&utm_source=waze_website&utm_medium=lm_share_location"
descargar_html_con_selenium(url)
