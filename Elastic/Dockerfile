FROM python:3.9-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /elastic

# Copiar archivos de Python
COPY requirements.txt .
COPY Insert.py .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Comando por defecto
CMD ["python", "Insert.py"] 