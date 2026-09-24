{{ config(materialized='table') }}

WITH base AS (
    SELECT
        boarding_id,
        user_hash,
        station_code,
        axis,
        (timestamp_utc::timestamptz AT TIME ZONE 'America/Guatemala') AS timestamp_local,
        cabin_number,
        fare::numeric(10,2) AS monto_gtq
    FROM {{ source('staging', 'aerometro_boardings') }}
),

con_duplicados AS (
    SELECT
        *,
        LAG(timestamp_local) OVER (
            PARTITION BY user_hash, station_code
            ORDER BY timestamp_local
        ) AS prev_ts
    FROM base
),

clasificado AS (
    SELECT
        *,
        CASE
            WHEN station_code IS NULL OR TRIM(station_code) = ''
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