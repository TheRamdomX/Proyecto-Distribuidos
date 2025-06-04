#!/bin/bash

# Crear directorios necesarios
mkdir -p /pig/data
mkdir -p /pig/results

# Esperar a que el archivo de Filter esté disponible
echo "Esperando a que el archivo de Filter esté disponible..."
max_attempts=12  # 12 intentos * 5 segundos = 60 segundos máximo
attempt=1

while [ ! -f "/filter/results/filtered_events/part-r-00000" ] && [ $attempt -le $max_attempts ]; do
    echo "Intento $attempt de $max_attempts: Esperando 5 segundos..."
    sleep 5
    attempt=$((attempt + 1))
done

if [ ! -f "/filter/results/filtered_events/part-r-00000" ]; then
    echo "❌ Error: El archivo de resultados de Filter no existe después de $max_attempts intentos"
    echo "Asegúrese de que el módulo Filter haya procesado los datos primero"
    exit 1
fi

# Transformar el archivo a CSV y copiarlo
echo "Transformando archivo a CSV y copiándolo..."
awk -F',' 'BEGIN {print "id,timestamp,latitude,longitude,event_type,comuna"} \
    {print $1 "," $2 "," $3 "," $4 "," $5 "," $6}' /filter/results/filtered_events/part-r-00000 > /pig/data/events.csv

# Asegurar permisos correctos
chmod 644 /pig/data/events.csv

# Verificar que el archivo existe
if [ -f "/pig/data/events.csv" ]; then
    echo "✅ Archivo CSV creado correctamente"
    # Ejecutar script Pig
    echo "Ejecutando script Pig..."
    pig -x local process_events.pig
else
    echo "❌ Error: No se pudo crear el archivo CSV"
    exit 1
fi