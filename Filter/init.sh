#!/bin/bash

# Crear directorios necesarios
mkdir -p /filter/data
mkdir -p /filter/results

# Exportar datos de MySQL a CSV
echo "Exportando datos de MySQL a CSV..."
mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "
SELECT id, timestamp, latitude, longitude, event_type, comuna 
FROM events 
INTO OUTFILE '/var/lib/mysql-files/events.csv' 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';"

# Copiar el archivo al directorio de datos
cp /var/lib/mysql-files/events.csv /filter/data/

# Asegurar permisos correctos
chmod 644 /filter/data/events.csv

# Verificar que el archivo existe
if [ -f "/filter/data/events.csv" ]; then
    echo "✅ Archivo CSV creado correctamente"
    # Ejecutar script Pig
    echo "Ejecutando script Pig..."
    pig -x local filter_events.pig
    chmod -R 777 /filter/results/filtered_events/
else
    echo "❌ Error: No se pudo crear el archivo CSV"
    exit 1
fi 