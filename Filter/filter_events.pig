REGISTER 'piggybank.jar';

-- Limpiar directorios de resultados anteriores
rmf /filter/results/filtered_events;

-- Cargar los eventos desde el archivo de texto
events = LOAD '/filter/data/events.txt' USING TextLoader() AS (line:chararray);

-- Parsear la línea para extraer los campos
parsed_events = FOREACH events GENERATE
    FLATTEN(STRSPLIT(line, ',')) AS (id:int, timestamp:chararray, latitude:double, longitude:double, event_type:chararray, comuna:chararray);

-- Agrupar eventos por tipo y ventana de tiempo (1 hora)
-- Convertir timestamp a Unix timestamp para facilitar el agrupamiento
events_with_ts = FOREACH parsed_events GENERATE
    id,
    ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')) as unix_ts,
    latitude,
    longitude,
    event_type,
    comuna;

-- Agrupar por tipo de evento y ventana de tiempo (1 hora)
grouped_by_time = GROUP events_with_ts BY (event_type, FLOOR(unix_ts/3600));

-- Para cada grupo, calcular la distancia entre eventos y eliminar duplicados
filtered_events = FOREACH grouped_by_time {
    -- Ordenar eventos por timestamp
    sorted_events = ORDER events_with_ts BY unix_ts;
    
    -- Generar pares de eventos para comparar
    event_pairs = CROSS sorted_events, sorted_events;
    
    -- Filtrar solo pares donde el segundo evento es posterior
    valid_pairs = FILTER event_pairs BY $0.unix_ts < $1.unix_ts;
    
    -- Calcular distancia entre eventos
    pairs_with_distance = FOREACH valid_pairs GENERATE
        $0.id as id1,
        $1.id as id2,
        SQRT(POW($0.latitude - $1.latitude, 2) + POW($0.longitude - $1.longitude, 2)) as distance;
    
    -- Identificar eventos duplicados (distancia < 0.0001)
    duplicates = FILTER pairs_with_distance BY distance < 0.0001;
    
    -- Obtener IDs de eventos duplicados
    duplicate_ids = FOREACH duplicates GENERATE id2;
    
    -- Filtrar eventos originales excluyendo duplicados
    unique_events = FILTER sorted_events BY NOT (id IN duplicate_ids);
    
    -- Generar resultado final
    GENERATE FLATTEN(unique_events);
}

-- Convertir de vuelta al formato original
final_events = FOREACH filtered_events GENERATE
    id,
    ToString(ToDate(unix_ts * 1000L, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')) as timestamp,
    latitude,
    longitude,
    event_type,
    comuna;

-- Guardar resultados
STORE final_events INTO '/filter/results/filtered_events' USING PigStorage(','); 