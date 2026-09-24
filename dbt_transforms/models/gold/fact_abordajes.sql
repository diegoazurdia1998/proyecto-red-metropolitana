{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

WITH tm AS (
    SELECT
        MD5('Transmetro:' || tarjeta) AS sk_usuario,
        TO_CHAR(timestamp_local, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        estacion::varchar AS codigo_estacion,
        linea::varchar AS linea_ruta,
        'Transmetro' AS modo,
        timestamp_local,
        monto_gtq
    FROM {{ ref('silver_transmetro_validado') }}
    WHERE regla_calidad = 'VALIDO'
),

tu AS (
    SELECT
        MD5('Transurbano:' || num_tarjeta) AS sk_usuario,
        TO_CHAR(timestamp_local, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        cod_parada::varchar AS codigo_estacion,
        ruta::varchar AS linea_ruta,
        'Transurbano' AS modo,
        timestamp_local,
        monto_gtq
    FROM {{ ref('silver_transurbano_validado') }}
    WHERE regla_calidad = 'VALIDO'
),

am AS (
    SELECT
        MD5('Aerometro:' || user_hash) AS sk_usuario,
        TO_CHAR(timestamp_local, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        station_code::varchar AS codigo_estacion,
        axis::varchar AS linea_ruta,
        'Aerometro' AS modo,
        timestamp_local,
        monto_gtq
    FROM {{ ref('silver_aerometro_validado') }}
    WHERE regla_calidad = 'VALIDO'
),

mr AS (
    SELECT
        MD5('MetroRiel:' || num_tarjeta) AS sk_usuario,
        TO_CHAR(timestamp_origen, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        estacion_origen::varchar AS codigo_estacion,
        'MetroRiel Troncal'::varchar AS linea_ruta,
        'MetroRiel' AS modo,
        timestamp_origen AS timestamp_local,
        monto_gtq
    FROM {{ ref('silver_metroriel_validado') }}
    WHERE regla_calidad = 'VALIDO'
),

todos AS (
    SELECT * FROM tm
    UNION ALL
    SELECT * FROM tu
    UNION ALL
    SELECT * FROM am
    UNION ALL
    SELECT * FROM mr
)

SELECT
    MD5(t.modo || ':' || t.sk_usuario || ':' || t.timestamp_local::varchar) AS sk_fact_abordaje,
    t.sk_usuario,
    t.sk_tiempo,
    COALESCE(dep.sk_estacion, '-1') AS sk_estacion,
    t.codigo_estacion,
    t.linea_ruta,
    t.modo,
    t.timestamp_local,
    t.monto_gtq,
    1 AS cantidad_abordajes
FROM todos t
LEFT JOIN {{ ref('dim_estacion_parada') }} dep
    ON t.codigo_estacion = dep.codigo_estacion
   AND t.modo = dep.sistema_modo