{{ config(materialized='table') }}

WITH base AS (
    SELECT
        num_tarjeta,
        cod_parada,
        ruta,
        TO_TIMESTAMP(fecha || ' ' || hora, 'DD/MM/YYYY HH24:MI:SS') AS timestamp_local,
        (monto_centavos / 100.0)::numeric(10,2) AS monto_gtq,
        cod_estado
    FROM {{ source('staging', 'transurbano_transacciones') }}
),

con_duplicados AS (
    SELECT
        *,
        LAG(timestamp_local) OVER (
            PARTITION BY num_tarjeta, cod_parada
            ORDER BY timestamp_local
        ) AS prev_ts
    FROM base
),

clasificado AS (
    SELECT
        *,
        CASE
            WHEN cod_parada IS NULL OR TRIM(cod_parada) = ''
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