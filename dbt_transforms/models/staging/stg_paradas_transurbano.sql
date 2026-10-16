{{ config(materialized='view') }}

SELECT
    TRIM(cod_parada)::varchar AS codigo_estacion,
    TRIM(descripcion)::varchar AS nombre_estacion,
    'Transurbano' AS sistema_modo,
    {{ normalizar_zona('sector') }} AS zona_normalizada,
    TRIM(ruta)::varchar AS linea_ruta
FROM {{ source('staging','tu_paradas') }}
WHERE cod_parada IS NOT NULL