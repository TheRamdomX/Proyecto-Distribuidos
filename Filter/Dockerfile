FROM openjdk:8-jdk

# Instalar dependencias necesarias
RUN apt-get update && \
    apt-get install -y wget default-mysql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Descargar e instalar Pig
RUN wget https://dlcdn.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -xzf pig-0.17.0.tar.gz && \
    mv pig-0.17.0 /opt/pig && \
    rm pig-0.17.0.tar.gz

# Descargar piggybank.jar
RUN wget https://repo1.maven.org/maven2/org/apache/pig/piggybank/0.17.0/piggybank-0.17.0.jar -O /opt/pig/lib/piggybank.jar

# Configurar variables de entorno
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$PIG_HOME/bin

# Crear directorios necesarios
RUN mkdir -p /filter/data /filter/results

# Copiar scripts
COPY filter_events.pig /filter/
COPY init.sh /filter/

# Dar permisos de ejecución al script
RUN chmod +x /filter/init.sh

# Establecer directorio de trabajo
WORKDIR /filter

# Ejecutar script de inicialización
CMD ["/filter/init.sh"] 