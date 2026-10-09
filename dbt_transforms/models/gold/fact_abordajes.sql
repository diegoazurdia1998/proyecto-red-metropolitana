{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

WITH tm AS (
    SELECT
        MD5(NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '')::bigint::text) AS sk_usuario,
        TO_CHAR(timestamp_local, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        estacion::varchar AS codigo_estacion,
        linea::varchar AS linea_ruta,
        'Transmetro' AS sistema_modo,
        timestamp_local,
        monto_gtq
    FROM {{ ref('silver_transmetro_validado') }}
    WHERE regla_calidad = 'VALIDO'
),

tu AS (
    SELECT
        MD5(NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '')::bigint::text) AS sk_usuario,
        TO_CHAR(timestamp_local, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        cod_parada::varchar AS codigo_estacion,
        ruta::varchar AS linea_ruta,
        'Transurbano' AS sistema_modo,
        timestamp_local,
        monto_gtq
    FROM {{ ref('silver_transurbano_validado') }}
    WHERE regla_calidad = 'VALIDO'
),

am AS (
    SELECT
        MD5(user_hash) AS sk_usuario,
        TO_CHAR(timestamp_local, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        station_code::varchar AS codigo_estacion,
        axis::varchar AS linea_ruta,
        'Aerometro' AS sistema_modo,
        timestamp_local,
        monto_gtq
    FROM {{ ref('silver_aerometro_validado') }}
    WHERE regla_calidad = 'VALIDO'
),

mr AS (
    SELECT
        MD5(NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '')::bigint::text) AS sk_usuario,
        TO_CHAR(timestamp_origen, 'YYYYMMDDHH24')::bigint AS sk_tiempo,
        estacion_origen::varchar AS codigo_estacion,
        'MetroRiel Troncal'::varchar AS linea_ruta,
        'MetroRiel' AS sistema_modo,
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
    MD5(t.sistema_modo || ':' || t.sk_usuario || ':' || t.timestamp_local::varchar) AS sk_fact_abordaje,
    t.sk_usuario,
    t.sk_tiempo,
    COALESCE(dep.sk_estacion::text, '-1') AS sk_estacion,
    t.codigo_estacion,
    t.linea_ruta,
    t.sistema_modo,
    t.timestamp_local,
    t.monto_gtq,
    1 AS cantidad_abordajes
FROM todos t
LEFT JOIN {{ ref('dim_estacion_parada') }} dep
    ON t.codigo_estacion = dep.codigo_estacion
   AND t.sistema_modo = dep.sistema_modo