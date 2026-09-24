{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

WITH fecha_ancla AS (
    -- Declaración de fecha de corte determinista calculada desde Silver
    SELECT
        GREATEST(
            (SELECT MAX(timestamp_local) FROM {{ ref('silver_transmetro_validado') }}),
            (SELECT MAX(timestamp_local) FROM {{ ref('silver_transurbano_validado') }}),
            (SELECT MAX(timestamp_local) FROM {{ ref('silver_aerometro_validado') }}),
            (SELECT MAX(timestamp_destino) FROM {{ ref('silver_metroriel_validado') }} views)
        ) AS fecha_corte
),

eventos_silver AS (
    -- Transmetro
    SELECT
        encode(sha256(tarjeta::bytea), 'hex') AS user_hash,
        'Transmetro' AS modo,
        timestamp_local AS fecha_hora,
        estacion AS codigo_ubicacion_origen,
        monto_gtq
    FROM {{ ref('silver_transmetro_validado') }}

    UNION ALL

    -- Transurbano
    SELECT
        encode(sha256(num_tarjeta::bytea), 'hex') AS user_hash,
        'Transurbano' AS modo,
        timestamp_local AS fecha_hora,
        cod_parada AS codigo_ubicacion_origen,
        monto_gtq
    FROM {{ ref('silver_transurbano_validado') }}

    UNION ALL

    -- Aerómetro (ya viene en hash)
    SELECT
        user_hash,
        'Aerometro' AS modo,
        timestamp_local AS fecha_hora,
        station_code AS codigo_ubicacion_origen,
        monto_gtq
    FROM {{ ref('silver_aerometro_validado') }}

    UNION ALL

    -- MetroRiel (evento de ingreso/origen)
    SELECT
        encode(sha256(num_tarjeta::bytea), 'hex') AS user_hash,
        'MetroRiel' AS modo,
        timestamp_origen AS fecha_hora,
        estacion_origen AS codigo_ubicacion_origen,
        monto_gtq
    FROM {{ ref('silver_metroriel_validado') }}
),

eventos_filtrados AS (
    SELECT
        e.*,
        f.fecha_corte,
        -- Lógica de hora pico en horario laboral (Lunes a Viernes de 06:00-08:59 y 17:00-19:59)
        CASE
            WHEN EXTRACT(ISODOW FROM e.fecha_hora) BETWEEN 1 AND 5
                 AND (EXTRACT(HOUR FROM e.fecha_hora) BETWEEN 6 AND 8
                      OR EXTRACT(HOUR FROM e.fecha_hora) BETWEEN 17 AND 19)
            THEN 1
            ELSE 0
        END AS es_hora_pico
    FROM eventos_silver e
    CROSS JOIN fecha_ancla f
    WHERE e.fecha_hora <= f.fecha_corte
),

preferencia_modo AS (
    SELECT
        user_hash,
        modo,
        ROW_NUMBER() OVER (PARTITION BY user_hash ORDER BY COUNT(*) DESC, modo ASC) AS rnk
    FROM eventos_filtrados
    GROUP BY user_hash, modo
),

preferencia_ubicacion AS (
    SELECT
        user_hash,
        codigo_ubicacion_origen,
        ROW_NUMBER() OVER (PARTITION BY user_hash ORDER BY COUNT(*) DESC, codigo_ubicacion_origen ASC) AS rnk
    FROM eventos_filtrados
    WHERE codigo_ubicacion_origen IS NOT NULL
    GROUP BY user_hash, codigo_ubicacion_origen
),

metricas_usuario AS (
    SELECT
        ef.user_hash,
        MAX(ef.fecha_corte) AS fecha_corte,

        -- Conteo en ventanas retrospectivas solicitadas
        COUNT(*) FILTER (WHERE ef.fecha_hora >= ef.fecha_corte - INTERVAL '7 days') AS viajes_7d,
        COUNT(*) FILTER (WHERE ef.fecha_hora >= ef.fecha_corte - INTERVAL '30 days') AS viajes_30d,
        COUNT(*) FILTER (WHERE ef.fecha_hora >= ef.fecha_corte - INTERVAL '90 days') AS viajes_90d,

        -- Cantidad de modos distintos
        COUNT(DISTINCT ef.modo) AS cantidad_modos_distintos,

        -- Recencia en días
        ROUND(
            EXTRACT(EPOCH FROM (MAX(ef.fecha_corte) - MAX(ef.fecha_hora))) / 86400.0,
            2
        ) AS dias_ultimo_viaje,

        -- Proporción de abordajes en hora pico
        ROUND(
            SUM(ef.es_hora_pico)::numeric / NULLIF(COUNT(*), 0),
            4
        ) AS pct_viajes_hora_pico,

        -- Gasto monetario
        ROUND(SUM(ef.monto_gtq)::numeric, 2) AS gasto_total_acumulado,
        ROUND(AVG(ef.monto_gtq)::numeric, 2) AS gasto_promedio_por_viaje

    FROM eventos_filtrados ef
    GROUP BY ef.user_hash
)

SELECT
    m.user_hash,
    m.fecha_corte,
    m.viajes_7d,
    m.viajes_30d,
    m.viajes_90d,
    pm.modo AS modo_frecuente,
    m.cantidad_modos_distintos,
    m.dias_ultimo_viaje,
    m.pct_viajes_hora_pico,
    COALESCE(pu.codigo_ubicacion_origen, 'Sin registro') AS ubicacion_origen_frecuente,
    m.gasto_total_acumulado,
    m.gasto_promedio_por_viaje
FROM metricas_usuario m
LEFT JOIN preferencia_modo pm
    ON m.user_hash = pm.user_hash AND pm.rnk = 1
LEFT JOIN preferencia_ubicacion pu
    ON m.user_hash = pu.user_hash AND pu.rnk = 1