REGISTER 'piggybank.jar';

-- Limpiar directorios de resultados anteriores
rmf /pig/results/event_counts;
rmf /pig/results/comuna_analysis;

-- Cargar los eventos desde el archivo de texto
events = LOAD '/pig/data/events.txt' USING TextLoader() AS (line:chararray);

-- Parsear la línea para extraer los campos
parsed_events = FOREACH events GENERATE
    FLATTEN(STRSPLIT(line, ',')) AS (id:int, timestamp:chararray, latitude:double, longitude:double, event_type:chararray, comuna:chararray);

-- Agrupar eventos por comuna y tipo
grouped_events = GROUP parsed_events BY (comuna, event_type);

-- Contar eventos por comuna y tipo
event_counts = FOREACH grouped_events GENERATE 
    group.comuna as comuna,
    group.event_type as tipo_evento,
    COUNT(parsed_events) as total_eventos;

-- Agrupar por comuna para obtener estadísticas
comuna_stats = GROUP event_counts BY comuna;

-- Calcular estadísticas por comuna
comuna_analysis = FOREACH comuna_stats {
    total = SUM(event_counts.total_eventos);
    tipos = COUNT(event_counts);
    GENERATE 
        group as comuna,
        total as total_eventos,
        tipos as tipos_eventos,
        event_counts.(tipo_evento, total_eventos) as desglose;
}

-- Guardar resultados
STORE comuna_analysis INTO '/pig/results/comuna_analysis' USING PigStorage(',');
STORE event_counts INTO '/pig/results/event_counts' USING PigStorage(','); 