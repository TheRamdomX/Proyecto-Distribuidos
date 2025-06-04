REGISTER '/opt/pig/lib/piggybank.jar';

-- Limpiar directorios de resultados anteriores
rmf /pig/results/event_counts;
rmf /pig/results/comuna_analysis;
rmf /pig/results/time_analysis;
rmf /pig/results/historical_analysis;

-- Cargar eventos desde archivo CSV
events = LOAD '/pig/data/events.csv' USING PigStorage(',') AS 
    (id:int, timestamp:chararray, latitude:double, longitude:double, event_type:chararray, comuna:chararray);

-- Filtrar la primera línea (encabezados) usando una condición más robusta
events = FILTER events BY (chararray)id != 'id' AND (chararray)id != '';

-- Convertir timestamp a Unix timestamp para análisis temporal
events_with_ts = FOREACH events GENERATE
    id,
    (long)timestamp as unix_ts,
    latitude,
    longitude,
    event_type,
    comuna;

-- Análisis por comuna y tipo de evento
grouped_events = GROUP events_with_ts BY (comuna, event_type);

-- Contar eventos por comuna y tipo
event_counts = FOREACH grouped_events GENERATE 
    group.comuna as comuna,
    group.event_type as tipo_evento,
    COUNT(events_with_ts) as total_eventos;

-- Análisis temporal por hora del día
events_by_hour = FOREACH events_with_ts GENERATE
    event_type,
    comuna,
    (int)((unix_ts % 86400) / 3600) as hora_dia;

hourly_stats = GROUP events_by_hour BY (event_type, hora_dia);
hourly_analysis = FOREACH hourly_stats GENERATE
    group.event_type as tipo_evento,
    group.hora_dia as hora,
    COUNT(events_by_hour) as total_eventos;

-- Análisis por comuna
comuna_stats = GROUP event_counts BY comuna;
comuna_analysis = FOREACH comuna_stats {
    total = SUM(event_counts.total_eventos);
    tipos = COUNT(event_counts);
    GENERATE 
        group as comuna,
        total as total_eventos,
        tipos as tipos_eventos;
}

-- Guardar resultados
STORE comuna_analysis INTO '/pig/results/comuna_analysis' USING PigStorage(',');
STORE event_counts INTO '/pig/results/event_counts' USING PigStorage(',');
STORE hourly_analysis INTO '/pig/results/historical_analysis' USING PigStorage(','); 