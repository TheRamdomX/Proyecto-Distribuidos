FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    wget unzip curl chromium-driver chromium \
    libgeos-dev \
    gdal-bin \
    libgdal-dev \
    && apt-get clean

# Configuración para Chrome
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMIUM_PATH=/usr/bin/chromium
ENV CHROME_PATH=/usr/bin/chromium

# Crear directorio para el usuario no root
RUN mkdir -p /home/chrome && \
    chown -R root:root /home/chrome

# Configurar variables de entorno para Chrome
ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99
ENV CHROME_OPTIONS="--headless --no-sandbox --disable-dev-shm-usage --disable-gpu"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY RM.geojson .
COPY scraper.py .

CMD ["python", "scraper.py"]