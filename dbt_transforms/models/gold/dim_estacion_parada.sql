{{ config(materialized='table') }}

WITH todas_las_estaciones AS (
    SELECT codigo_estacion, nombre_estacion, sistema_modo, zona_normalizada FROM {{ ref('stg_estaciones_transmetro') }}
    UNION ALL
    SELECT codigo_estacion, nombre_estacion, sistema_modo, zona_normalizada FROM {{ ref('stg_estaciones_aerometro') }}
    UNION ALL
    SELECT codigo_estacion, nombre_estacion, sistema_modo, zona_normalizada FROM {{ ref('stg_estaciones_metroriel') }}
    UNION ALL
    SELECT codigo_estacion, nombre_estacion, sistema_modo, zona_normalizada FROM {{ ref('stg_paradas_transurbano') }}
)

SELECT
    MD5(sistema_modo || ':' || codigo_estacion) AS sk_estacion,
    codigo_estacion,
    nombre_estacion,
    sistema_modo,
    zona_normalizada
FROM todas_las_estaciones