FROM apache/pig:latest

# Instalar cliente MySQL
RUN apt-get update && \
    apt-get install -y default-mysql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Crear directorios necesarios
RUN mkdir -p /filter/data /filter/results

# Copiar los scripts
COPY filter_events.pig /filter/
COPY init.sh /filter/

# Dar permisos de ejecución al script de inicialización
RUN chmod +x /filter/init.sh

# Establecer el directorio de trabajo
WORKDIR /filter

# Comando por defecto
CMD ["/bin/bash", "-c", "./init.sh && pig -f filter_events.pig"] 