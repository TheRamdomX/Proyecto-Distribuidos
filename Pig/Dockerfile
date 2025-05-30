FROM openjdk:8-jdk

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y \
    default-mysql-client \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Instalar Apache Pig
ENV PIG_VERSION=0.17.0
ENV PIG_HOME=/opt/pig
RUN wget https://dlcdn.apache.org/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz && \
    mv pig-${PIG_VERSION} ${PIG_HOME} && \
    rm pig-${PIG_VERSION}.tar.gz

# Descargar Piggybank
ENV PIGGYBANK_VERSION=0.17.0
RUN mkdir -p /pig && wget https://repo1.maven.org/maven2/org/apache/pig/piggybank/${PIGGYBANK_VERSION}/piggybank-${PIGGYBANK_VERSION}.jar -O /pig/piggybank.jar

# Configurar variables de entorno
ENV PATH=${PIG_HOME}/bin:${PATH}

# Crear directorios necesarios
WORKDIR /pig
RUN mkdir -p results data

# Copiar scripts
COPY process_events.pig .
COPY export_events.sh .

# Dar permisos de ejecución al script de exportación
RUN chmod +x export_events.sh

# Comando por defecto
CMD ["/bin/bash", "-c", "./export_events.sh && pig -x local process_events.pig"] 