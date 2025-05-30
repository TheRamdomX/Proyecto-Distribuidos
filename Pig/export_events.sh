#!/bin/bash

# Limpiar directorios de resultados anteriores
rm -rf /pig/results/event_counts/*
rm -rf /pig/results/comuna_analysis/*

# Exportar datos de MySQL a un archivo de texto con comas como separadores
mysql -h mysql -u user -ppassword waze_db -e "SELECT * FROM events" | tr '\t' ',' > /pig/data/events.txt

# Eliminar la primera línea (encabezado)
sed -i '1d' /pig/data/events.txt

# Asegurarse de que el archivo tenga los permisos correctos
chmod 644 /pig/data/events.txt