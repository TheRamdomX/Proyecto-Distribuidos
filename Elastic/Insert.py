import os
import pandas as pd
import shutil
from elasticsearch import Elasticsearch, helpers

# Conexión a Elasticsearch
es = Elasticsearch("http://elasticsearch:9200")

def delete_index_if_exists(index_name):
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"🗑️ Índice '{index_name}' eliminado antes de la inserción.")

def create_index_if_not_exists(index_name, mapping):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={"mappings": mapping})

def bulk_index(df, index_name, transform_func):
    try:
        actions = []
        for _, row in df.iterrows():
            try:
                action = {
                    "_index": index_name,
                    "_source": transform_func(row)
                }
                actions.append(action)
            except Exception as e:
                print(f"⚠️  Error transformando fila: {row}, Error: {e}")
                continue
        
        if actions:
            print(f"📊 Intentando indexar {len(actions)} documentos en {index_name}...")
            success, failed = 0, 0
            for ok, result in helpers.streaming_bulk(es, actions, chunk_size=100, raise_on_error=False):
                if ok:
                    success += 1
                else:
                    failed += 1
                    if failed <= 5:  # Mostrar solo los primeros 5 errores
                        print(f"❌ Error indexando documento: {result}")
            
            print(f"✅ {success} documentos indexados exitosamente en {index_name}")
            if failed > 0:
                print(f"⚠️  {failed} documentos fallaron al indexarse en {index_name}")
        else:
            print(f"⚠️  No hay documentos para indexar en {index_name}")
            
    except Exception as e:
        print(f"❌ Error en bulk_index para {index_name}: {e}")
        raise

def copy_events_files():
    try:
        os.makedirs("data", exist_ok=True)
        
        # Copiar archivo de Filter como Events.csv
        if os.path.exists("/filter/data/events.csv"):
            shutil.copy2("/filter/data/events.csv", "data/Events.csv")
            print("✅ Archivo copiado: /filter/data/events.csv → data/Events.csv")
        else:
            print("⚠️  No se encontró el archivo /filter/data/events.csv")
        
        # Copiar archivo de Pig como Events_Filtered.csv
        if os.path.exists("/pig/data/events.csv"):
            shutil.copy2("/pig/data/events.csv", "data/Events_Filtered.csv")
            print("✅ Archivo copiado: /pig/data/events.csv → data/Events_Filtered.csv")
        else:
            print("⚠️  No se encontró el archivo /pig/data/events.csv")
            
    except Exception as e:
        print(f"❌ Error al copiar archivos: {e}")

def convert_part_file_to_csv(input_file, output_file):
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(input_file, 'r', encoding='utf-8') as infile:
            lines = infile.readlines()
        
        if "comuna_analysis" in input_file:
            headers = "comuna,total_eventos,tipos_distintos"
        elif "event_counts" in input_file:
            headers = "comuna,tipo,cantidad"
        elif "historical_analysis" in input_file:
            headers = "tipo,hora,cantidad"
        else:
            headers = "columna1,columna2,columna3"
        
        with open(output_file, 'w', encoding='utf-8') as outfile:
            outfile.write(headers + "\n")
            
            for line in lines:
                line = line.strip()
                if line: 
                    outfile.write(line + "\n")
        
        print(f"✅ Archivo CSV creado correctamente: {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Error al convertir archivo: {e}")
        return False

def clean_and_validate_data(df, expected_columns):

    # Verificar que todas las columnas esperadas estén presentes
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"⚠️  Columnas faltantes: {missing_columns}")
        return None
    
    initial_count = len(df)
    df_clean = df.dropna(subset=expected_columns)
    final_count = len(df_clean)
    
    if initial_count != final_count:
        print(f"⚠️  Se eliminaron {initial_count - final_count} filas con valores nulos")
    
    print(f"✅ Datos validados: {final_count} filas válidas")
    return df_clean

# COPIA DE ARCHIVOS EVENTS.CSV
print("Copiando archivos events.csv...")
copy_events_files()

print("Convirtiendo archivos part-r-00000 a CSV...")

# Convertir archivo de análisis por comuna
convert_part_file_to_csv(
    "/pig/results/comuna_analysis/part-r-00000",
    "data/comuna_analysis/comuna_analysis.csv"
)

# Convertir archivo de conteo de eventos por comuna y tipo
convert_part_file_to_csv(
    "/pig/results/event_counts/part-r-00000",
    "data/event_counts/event_counts.csv"
)

# Convertir archivo de análisis histórico por hora
convert_part_file_to_csv(
    "/pig/results/historical_analysis/part-r-00000",
    "data/historical_analysis/historical_analysis.csv"
)

eventos_mapping = {
    "properties": {
        "timestamp": {"type": "date"},
        "tipo": {"type": "keyword"},
        "comuna": {"type": "keyword"},
        "location": {"type": "geo_point"}
    }
}

def transform_evento(row):
    try:
        # Convertir ID a entero, manejar casos donde no sea válido
        try:
            event_id = int(row["id"])
        except (ValueError, TypeError):
            event_id = 0
        
        # Validar coordenadas
        try:
            lat = float(row["lat"])
            lon = float(row["lon"])
        except (ValueError, TypeError):
            lat = 0.0
            lon = 0.0
        
        return {
            "id": event_id,
            "timestamp": str(row["timestamp"]),
            "tipo": str(row["tipo"]),
            "comuna": str(row["comuna"]),
            "location": {"lat": lat, "lon": lon}
        }
    except Exception as e:
        print(f"⚠️  Error transformando evento: {row}, Error: {e}")
        # Retornar un documento por defecto
        return {
            "id": 0,
            "timestamp": "1970-01-01T00:00:00",
            "tipo": "DESCONOCIDO",
            "comuna": "DESCONOCIDA",
            "location": {"lat": 0.0, "lon": 0.0}
        }

# 1. EVENTOS CRUDOS
if os.path.exists("data/Events.csv"):
    df_crudos = pd.read_csv("data/Events.csv", header=None, names=["id", "timestamp", "lat", "lon", "tipo", "comuna"])
    # Convertir timestamp a formato ISO
    df_crudos['timestamp'] = pd.to_datetime(df_crudos['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_crudos_clean = clean_and_validate_data(df_crudos, ["id", "timestamp", "tipo", "comuna"])
    if df_crudos_clean is not None:
        delete_index_if_exists("eventos-crudos")
        create_index_if_not_exists("eventos-crudos", eventos_mapping)
        bulk_index(df_crudos_clean, "eventos-crudos", transform_evento)
        print("✅ Eventos crudos cargados en Elasticsearch")
    else:
        print("⚠️  No se pudo procesar el archivo data/Events.csv - omitiendo carga de eventos crudos")
else:
    print("⚠️  No se encontró el archivo data/Events.csv - omitiendo carga de eventos crudos")

# 2. EVENTOS FILTRADOS
if os.path.exists("data/Events_Filtered.csv"):
    df_filtrados = pd.read_csv("data/Events_Filtered.csv")
    df_filtrados['timestamp'] = pd.to_datetime(df_filtrados['timestamp'], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_filtrados = df_filtrados.rename(columns={
        'latitude': 'lat',
        'longitude': 'lon',
        'event_type': 'tipo'
    })
    df_filtrados_clean = clean_and_validate_data(df_filtrados, ["id", "timestamp", "tipo", "comuna"])
    if df_filtrados_clean is not None:
        delete_index_if_exists("eventos-filtrados")
        create_index_if_not_exists("eventos-filtrados", eventos_mapping)
        bulk_index(df_filtrados_clean, "eventos-filtrados", transform_evento)
        print("✅ Eventos filtrados cargados en Elasticsearch")
    else:
        print("⚠️  No se pudo procesar el archivo data/Events_Filtered.csv - omitiendo carga de eventos filtrados")
else:
    print("⚠️  No se encontró el archivo data/Events_Filtered.csv - omitiendo carga de eventos filtrados")

# 3. EVENTOS POR COMUNA
if os.path.exists("data/comuna_analysis/comuna_analysis.csv"):
    df_comuna_total = pd.read_csv("data/comuna_analysis/comuna_analysis.csv")
    df_comuna_total_clean = clean_and_validate_data(df_comuna_total, ["comuna", "total_eventos", "tipos_distintos"])
    if df_comuna_total_clean is not None:
        delete_index_if_exists("eventos-por-comuna")
        create_index_if_not_exists("eventos-por-comuna", {
            "properties": {
                "comuna": {"type": "keyword"},
                "total_eventos": {"type": "integer"},
                "tipos_distintos": {"type": "integer"}
            }
        })
        bulk_index(df_comuna_total_clean, "eventos-por-comuna", lambda row: row.to_dict())
        print("✅ Análisis por comuna cargado en Elasticsearch")
    else:
        print("⚠️  No se pudo procesar el archivo data/comuna_analysis/comuna_analysis.csv - omitiendo carga de análisis por comuna")
else:
    print("⚠️  No se encontró el archivo data/comuna_analysis/comuna_analysis.csv - omitiendo carga de análisis por comuna")

# 4. EVENTOS POR COMUNA Y TIPO
if os.path.exists("data/event_counts/event_counts.csv"):
    df_comuna_tipo = pd.read_csv("data/event_counts/event_counts.csv")
    df_comuna_tipo_clean = clean_and_validate_data(df_comuna_tipo, ["comuna", "tipo", "cantidad"])
    if df_comuna_tipo_clean is not None:
        delete_index_if_exists("eventos-comuna-tipo")
        create_index_if_not_exists("eventos-comuna-tipo", {
            "properties": {
                "comuna": {"type": "keyword"},
                "tipo": {"type": "keyword"},
                "cantidad": {"type": "integer"}
            }
        })
        bulk_index(df_comuna_tipo_clean, "eventos-comuna-tipo", lambda row: row.to_dict())
        print("✅ Conteo de eventos por comuna y tipo cargado en Elasticsearch")
    else:
        print("⚠️  No se pudo procesar el archivo data/event_counts/event_counts.csv - omitiendo carga de conteo de eventos")
else:
    print("⚠️  No se encontró el archivo data/event_counts/event_counts.csv - omitiendo carga de conteo de eventos")

# 5. DISTRIBUCIÓN HORARIA
if os.path.exists("data/historical_analysis/historical_analysis.csv"):
    df_hora = pd.read_csv("data/historical_analysis/historical_analysis.csv")
    df_hora_clean = clean_and_validate_data(df_hora, ["tipo", "hora", "cantidad"])
    if df_hora_clean is not None:
        delete_index_if_exists("eventos-por-hora")
        create_index_if_not_exists("eventos-por-hora", {
            "properties": {
                "tipo": {"type": "keyword"},
                "hora": {"type": "integer"},
                "cantidad": {"type": "integer"}
            }
        })
        bulk_index(df_hora_clean, "eventos-por-hora", lambda row: row.to_dict())
        print("✅ Análisis histórico por hora cargado en Elasticsearch")
    else:
        print("⚠️  No se pudo procesar el archivo data/historical_analysis/historical_analysis.csv - omitiendo carga de análisis histórico")
else:
    print("⚠️  No se encontró el archivo data/historical_analysis/historical_analysis.csv - omitiendo carga de análisis histórico")

print("✅ Datos cargados correctamente en Elasticsearch.")
