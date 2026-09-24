{{ config(materialized='view') }}

SELECT
    TRIM(estacion_id)::varchar AS codigo_estacion,
    TRIM(nombre)::varchar AS nombre_estacion,
    'Transmetro' AS sistema_modo,
    {{ normalizar_zona('zona') }} AS zona_normalizada,
    TRIM(linea)::varchar AS linea_ruta
FROM {{ ref('tm_estaciones') }}
WHERE estacion_id IS NOT NULL