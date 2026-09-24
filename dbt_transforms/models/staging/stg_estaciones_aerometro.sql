{{ config(materialized='view') }}

SELECT
    TRIM(station_code)::varchar AS codigo_estacion,
    TRIM(station_name)::varchar AS nombre_estacion,
    'Aerometro' AS sistema_modo,
    {{ normalizar_zona('district') }} AS zona_normalizada,
    TRIM(axis)::varchar AS linea_ruta
FROM {{ ref('am_estaciones') }}
WHERE station_code IS NOT NULL