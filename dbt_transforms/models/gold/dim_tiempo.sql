{{ config(materialized='table') }}

WITH spine AS (
    SELECT
        ts AS timestamp_local
    FROM GENERATE_SERIES(
        '2026-05-01 00:00:00'::timestamp,
        '2026-07-01 23:59:59'::timestamp,
        INTERVAL '1 hour'
    ) ts
)

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
             AND (EXTRACT(HOUR FROM timestamp_local) BETWEEN 6 AND 8
                  OR EXTRACT(HOUR FROM timestamp_local) BETWEEN 17 AND 19)
        THEN TRUE
        ELSE FALSE
    END AS es_hora_pico
FROM spine