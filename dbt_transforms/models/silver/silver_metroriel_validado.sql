{{ config(materialized='table') }}

WITH base AS (
    SELECT
        trip_id,
        card AS num_tarjeta,
        (entry->>'station')::varchar AS estacion_origen,
        (entry->>'ts')::timestamp AS timestamp_origen,
        (exit->>'station')::varchar AS estacion_destino,
        (exit->>'ts')::timestamp AS timestamp_destino,
        fare_gtq::numeric(10,2) AS monto_gtq,
        duration_s::int AS duracion_segundos
    FROM {{ source('staging', 'metroriel_viajes') }}
),

clasificado AS (
    SELECT
        *,
        CASE
            WHEN estacion_origen IS NULL OR TRIM(estacion_origen) = ''
                THEN 'PARADA_NULA'
            WHEN estacion_destino IS NULL OR timestamp_destino IS NULL
                THEN 'VIAJE_SIN_SALIDA'
            WHEN timestamp_origen > CURRENT_TIMESTAMP OR timestamp_destino > CURRENT_TIMESTAMP
                THEN 'FECHA_FUTURA'
            ELSE 'VALIDO'
        END AS regla_calidad
    FROM base
)

SELECT * FROM clasificado