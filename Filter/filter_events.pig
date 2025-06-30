REGISTER '/opt/pig/lib/piggybank.jar';

-- Limpiar resultados anteriores
rmf /filter/results/filtered_events;

-- Cargar eventos desde CSV
events = LOAD '/filter/data/events.csv' USING PigStorage(',') AS 
    (id:int, timestamp:chararray, latitude:double, longitude:double, event_type:chararray, comuna:chararray);

-- Convertir timestamp a Unix time
events_with_ts = FOREACH events GENERATE
    id,
    ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd HH:mm:ss')) AS unix_ts,
    latitude,
    longitude,
    event_type,
    comuna;

-- Redondear coordenadas para agrupar por proximidad
events_grouped = FOREACH events_with_ts GENERATE
    id,
    unix_ts,
    latitude,
    longitude,
    event_type,
    comuna,
    ROUND(latitude * 10000) AS lat_rounded,
    ROUND(longitude * 10000) AS lon_rounded;

-- Agrupar por tipo de evento y coordenadas redondeadas
grouped = GROUP events_grouped BY (event_type, lat_rounded, lon_rounded);

-- Aplanar los grupos para procesar
flattened_events = FOREACH grouped GENERATE FLATTEN(events_grouped);

-- Crear dos copias con alias diferentes
events_copy1 = FOREACH flattened_events GENERATE *;
events_copy2 = FOREACH flattened_events GENERATE *;

-- Crear pares usando JOIN consigo mismo en las mismas coordenadas
self_join = JOIN events_copy1 BY (event_type, lat_rounded, lon_rounded), 
            events_copy2 BY (event_type, lat_rounded, lon_rounded);

-- Filtrar pares que cumplen criterios de duplicación
filtered_pairs = FILTER self_join BY 
    events_copy1::events_grouped::id < events_copy2::events_grouped::id AND
    ABS(events_copy1::events_grouped::unix_ts - events_copy2::events_grouped::unix_ts) <= 300 AND
    ABS(events_copy1::events_grouped::latitude - events_copy2::events_grouped::latitude) < 0.0001 AND
    ABS(events_copy1::events_grouped::longitude - events_copy2::events_grouped::longitude) < 0.0001;

-- Extraer IDs de eventos duplicados
duplicate_ids = FOREACH filtered_pairs GENERATE events_copy2::events_grouped::id AS id;

-- LEFT OUTER JOIN para marcar duplicados
joined = JOIN events_with_ts BY id LEFT OUTER, duplicate_ids BY id;

-- Filtrar eventos no duplicados
filtered_events = FILTER joined BY duplicate_ids::id IS NULL;

-- Convertir timestamp Unix de vuelta a formato legible
events_with_readable_ts = FOREACH filtered_events GENERATE
    events_with_ts::id AS id,
    (long)events_with_ts::unix_ts AS unix_ts_long,
    events_with_ts::latitude AS latitude,
    events_with_ts::longitude AS longitude,
    events_with_ts::event_type AS event_type,
    events_with_ts::comuna AS comuna;

-- Formato de salida
final = FOREACH events_with_readable_ts GENERATE
    id,
    unix_ts_long AS timestamp,
    latitude,
    longitude,
    event_type,
    comuna;

-- Guardar resultados
STORE final INTO '/filter/results/filtered_events' USING PigStorage(',');
