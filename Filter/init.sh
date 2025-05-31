#!/bin/bash

# Limpiar directorios de resultados anteriores
rm -rf results/filtered_events/*

# Crear directorios necesarios
mkdir -p data results

# Dar permisos necesarios
chmod 777 data results

# Exportar datos de MySQL a un archivo de texto con comas como separadores
mysql -h mysql -u user -ppassword waze_db -e "SELECT * FROM events" | tr '\t' ',' > data/events.txt

# Eliminar la primera línea (encabezado)
sed -i '1d' data/events.txt

# Asegurarse de que el archivo tenga los permisos correctos
chmod 644 data/events.txt

echo "✅ Directorios inicializados y datos exportados correctamente" 