
  
    

  create  table "calidad_aire"."public"."clean_pollution__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    -- El valor de contaminación
    CAST(data->>'no2' AS NUMERIC) as indice_aqi,
    
    -- El nombre de la estación
    COALESCE(data->>'nombre', 'Estación ' || COALESCE(data->>'objectid', 'X')) as nombre_estacion,
    
    -- EXTRAEMOS LAS COORDENADAS (¡La parte nueva!)
    -- La API suele devolver geo_point_2d como un objeto JSON {"lon": X, "lat": Y}
    CAST(data->'geo_point_2d'->>'lat' AS NUMERIC) as latitud,
    CAST(data->'geo_point_2d'->>'lon' AS NUMERIC) as longitud,

    ingestion_time

FROM raw_pollution
WHERE data->>'no2' IS NOT NULL
-- Solo nos quedamos con filas que tengan coordenadas para poder calcular distancias
AND data->'geo_point_2d'->>'lat' IS NOT NULL
  );
  