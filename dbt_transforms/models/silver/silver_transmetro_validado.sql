{{ config(materialized='table') }}

WITH base AS (
    SELECT
        id_validacion,
        tarjeta,
        estacion,
        linea,
        timestamp_validacion::timestamp AS timestamp_local,
        monto_gtq::numeric(10,2) AS monto_gtq
    FROM {{ source('staging', 'transmetro_validaciones') }}
),

con_duplicados AS (
    SELECT
        *,
        LAG(timestamp_local) OVER (
            PARTITION BY tarjeta, estacion
            ORDER BY timestamp_local
        ) AS prev_ts
    FROM base
),

clasificado AS (
    SELECT
        *,
        CASE
            WHEN estacion IS NULL OR TRIM(estacion) = ''
                THEN 'PARADA_NULA'
            WHEN timestamp_local > CURRENT_TIMESTAMP
                THEN 'FECHA_FUTURA'
            WHEN prev_ts IS NOT NULL AND EXTRACT(EPOCH FROM (timestamp_local - prev_ts)) <= 120
                THEN 'DUPLICADO_TORNIQUETE'
            ELSE 'VALIDO'
        END AS regla_calidad
    FROM con_duplicados
)

SELECT * FROM clasificado