{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

SELECT
    smv.trip_id AS id_viaje,
    MD5('MetroRiel:' || smv.num_tarjeta) AS sk_usuario,
    TO_CHAR(smv.timestamp_origen, 'YYYYMMDDHH24')::bigint AS sk_tiempo_origen,
    TO_CHAR(smv.timestamp_destino, 'YYYYMMDDHH24')::bigint AS sk_tiempo_destino,
    COALESCE(dep_o.sk_estacion, '-1') AS sk_estacion_origen,
    COALESCE(dep_d.sk_estacion, '-1') AS sk_estacion_destino,
    smv.estacion_origen::varchar,
    smv.estacion_destino::varchar,
    smv.timestamp_origen,
    smv.timestamp_destino,
    smv.duracion_segundos,
    ROUND((smv.duracion_segundos / 60.0)::numeric, 2) AS duracion_minutos,
    smv.monto_gtq,
    1 AS cantidad_viajes
FROM {{ ref('silver_metroriel_validado') }} smv
LEFT JOIN {{ ref('dim_estacion_parada') }} dep_o
    ON smv.estacion_origen::varchar = dep_o.codigo_estacion
   AND dep_o.sistema_modo = 'MetroRiel'
LEFT JOIN {{ ref('dim_estacion_parada') }} dep_d
    ON smv.estacion_destino::varchar = dep_d.codigo_estacion
   AND dep_d.sistema_modo = 'MetroRiel'