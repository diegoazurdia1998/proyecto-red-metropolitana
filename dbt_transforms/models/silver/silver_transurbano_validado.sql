{{ config(materialized='table') }}

WITH base AS (
    SELECT
        num_tarjeta,
        NULLIF(
            UPPER(REGEXP_REPLACE(TRIM(cod_parada), '\s+', '', 'g')),
            ''
        ) AS cod_parada_limpio,
        UPPER(TRIM(ruta)) AS ruta_limpia,
        TO_TIMESTAMP(fecha || ' ' || hora, 'DD/MM/YYYY HH24:MI:SS') AS timestamp_local,
        (monto_centavos / 100.0)::numeric(10,2) AS monto_gtq,
        cod_estado
    FROM {{ source('staging', 'transurbano_transacciones') }}
),

base2 AS (
    SELECT
        num_tarjeta,
        CASE
            WHEN UPPER(cod_parada_limpio) IN ('NAN', 'NULL', 'NONE', 'NA')
                THEN NULL
            ELSE cod_parada_limpio
        END AS cod_parada,
        ruta_limpia AS ruta,
        timestamp_local,
        monto_gtq,
        cod_estado
    FROM base
),

con_duplicados AS (
    SELECT
        *,
        LAG(timestamp_local) OVER (
            PARTITION BY num_tarjeta, cod_parada
            ORDER BY timestamp_local
        ) AS prev_ts
    FROM base2
),

clasificado AS (
    SELECT
        *,
        CASE
            WHEN cod_parada IS NULL THEN 'PARADA_NULA'
            WHEN timestamp_local > '{{ var("fecha_corte_referencia", "2026-07-15 23:59:59") }}'::timestamp
                THEN 'FECHA_FUTURA'
            WHEN prev_ts IS NOT NULL
             AND EXTRACT(EPOCH FROM (timestamp_local - prev_ts)) <= 120
                THEN 'DUPLICADO_TORNIQUETE'
            ELSE 'VALIDO'
        END AS regla_calidad
    FROM con_duplicados
)

SELECT * FROM clasificado