REGISTER '/opt/pig/lib/piggybank.jar';

-- Limpiar directorios de resultados anteriores
rmf /filter/results/filtered_events;

-- Cargar eventos desde archivo CSV
events = LOAD '/filter/data/events.csv' USING PigStorage(',') AS 
    (id:int, timestamp:chararray, latitude:double, longitude:double, event_type:chararray, comuna:chararray);

-- Convertir timestamp a Unix timestamp para facilitar el agrupamiento
events_with_ts = FOREACH events GENERATE
    id,
    ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd HH:mm:ss')) as unix_ts,
    latitude,
    longitude,
    event_type,
    comuna;

-- Agrupar por tipo de evento y ventana de tiempo (1 hora)
grouped_by_time = GROUP events_with_ts BY (event_type, FLOOR(unix_ts/3600));

-- Para cada grupo, ordenar eventos por timestamp
sorted_events = FOREACH grouped_by_time {
    sorted = ORDER events_with_ts BY unix_ts;
    GENERATE FLATTEN(sorted);
}

-- Generar pares de eventos para comparar
event_pairs = CROSS sorted_events, sorted_events;

-- Filtrar solo pares donde el segundo evento es posterior
valid_pairs = FILTER event_pairs BY sorted_events::sorted::unix_ts < sorted_events::sorted::unix_ts;

-- Calcular distancia entre eventos
pairs_with_distance = FOREACH valid_pairs GENERATE
    sorted_events::sorted::id as id1,
    sorted_events::sorted::id as id2,
    SQRT((sorted_events::sorted::latitude - sorted_events::sorted::latitude) * (sorted_events::sorted::latitude - sorted_events::sorted::latitude) + 
         (sorted_events::sorted::longitude - sorted_events::sorted::longitude) * (sorted_events::sorted::longitude - sorted_events::sorted::longitude)) as distance;

-- Identificar eventos duplicados (distancia < 0.001)
duplicates = FILTER pairs_with_distance BY distance < 0.001;

-- Obtener IDs de eventos duplicados
duplicate_ids = FOREACH duplicates GENERATE id2;

-- Unir eventos originales con duplicados usando LEFT OUTER JOIN
joined_events = JOIN sorted_events BY sorted::id LEFT OUTER, duplicate_ids BY id2;

-- Filtrar eventos que no tienen duplicados (id2 es null)
unique_events = FILTER joined_events BY id2 IS NULL;

-- Convertir de vuelta al formato original
final_events = FOREACH unique_events GENERATE
    sorted_events::sorted::id as id,
    sorted_events::sorted::unix_ts as unix_ts,
    sorted_events::sorted::latitude as latitude,
    sorted_events::sorted::longitude as longitude,
    sorted_events::sorted::event_type as event_type,
    sorted_events::sorted::comuna as comuna;

-- Guardar resultados en archivo
STORE final_events INTO '/filter/results/filtered_events' USING PigStorage(','); 
