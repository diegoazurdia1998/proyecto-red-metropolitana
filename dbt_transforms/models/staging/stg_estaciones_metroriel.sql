{{ config(materialized='view') }}

SELECT
    TRIM(id_estacion::text)::varchar AS codigo_estacion,
    TRIM(nombre_estacion)::varchar AS nombre_estacion,
    'MetroRiel' AS sistema_modo,
    {{ normalizar_zona('zona_nombre') }} AS zona_normalizada,
    'MetroRiel Troncal' AS linea_ruta
FROM {{ ref('mr_estaciones') }}
WHERE id_estacion IS NOT NULL