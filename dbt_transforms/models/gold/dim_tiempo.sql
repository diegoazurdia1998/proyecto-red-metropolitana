{{ config(materialized='table') }}

WITH rango_silvers AS (
    SELECT
        MIN(min_ts) AS min_ts,
        MAX(max_ts) AS max_ts
    FROM (
        SELECT MIN(timestamp_local) AS min_ts, MAX(timestamp_local) AS max_ts
        FROM {{ ref('silver_transmetro_validado') }}
        WHERE regla_calidad = 'VALIDO'
        UNION ALL
        SELECT MIN(timestamp_local), MAX(timestamp_local)
        FROM {{ ref('silver_transurbano_validado') }}
        WHERE regla_calidad = 'VALIDO'
        UNION ALL
        SELECT MIN(timestamp_local), MAX(timestamp_local)
        FROM {{ ref('silver_aerometro_validado') }}
        WHERE regla_calidad = 'VALIDO'
        UNION ALL
        SELECT MIN(timestamp_origen), MAX(timestamp_origen)
        FROM {{ ref('silver_metroriel_validado') }}
        WHERE regla_calidad = 'VALIDO'
    ) x
),

-- Salvaguarda: si no hay data, usa un rango por defecto
limites AS (
    SELECT
        COALESCE(
            DATE_TRUNC('hour', min_ts) - INTERVAL '1 month',
            '2025-01-01 00:00:00'::timestamp
        ) AS inicio,
        COALESCE(
            DATE_TRUNC('hour', max_ts) + INTERVAL '1 month',
            '2027-12-31 23:00:00'::timestamp
        ) AS fin
    FROM rango_silvers
),

spine AS (
    SELECT
        ts AS timestamp_local
    FROM limites,
         GENERATE_SERIES(limites.inicio, limites.fin, INTERVAL '1 hour') ts
),

dim AS (
    SELECT
        TO_CHAR(timestamp_local, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        timestamp_local,
        timestamp_local::date AS fecha,
        EXTRACT(YEAR FROM timestamp_local)::int AS anio,
        EXTRACT(MONTH FROM timestamp_local)::int AS mes,
        EXTRACT(DAY FROM timestamp_local)::int AS dia,
        EXTRACT(HOUR FROM timestamp_local)::int AS hora,
        TO_CHAR(timestamp_local, 'Day') AS dia_nombre,
        CASE
            WHEN EXTRACT(ISODOW FROM timestamp_local) IN (6, 7) THEN FALSE
            ELSE TRUE
        END AS es_dia_habil,
        CASE
            WHEN EXTRACT(ISODOW FROM timestamp_local) NOT IN (6, 7)
                 AND EXTRACT(HOUR FROM timestamp_local) IN (6, 7, 17, 18)
            THEN TRUE
            ELSE FALSE
        END AS es_hora_pico
    FROM spine
)

SELECT * FROM dim

UNION ALL

SELECT
    '-1' AS sk_tiempo,
    NULL::timestamp AS timestamp_local,
    NULL::date AS fecha,
    -1 AS anio,
    -1 AS mes,
    -1 AS dia,
    -1 AS hora,
    'DESCONOCIDO' AS dia_nombre,
    FALSE AS es_dia_habil,
    FALSE AS es_hora_pico