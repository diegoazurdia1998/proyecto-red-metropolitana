{{ config(materialized='table') }}

{#
  Features por fecha de corte para el Proyecto 2 (rubric 1.3).
  - Parametrizada por var('fecha_corte'). Default: 2026-07-01 00:00:00 (corte A).
  - Ventana retrospectiva fija de 28 días.
  - Solo registros VALIDO (cuarentena excluida por regla_calidad = 'VALIDO').
  - NO incluye etiqueta (ver p2_etiquetas_por_corte.sql).
  - NO incluye filtro de población (ver p2_poblacion_por_corte.sql).
#}

{% set fecha_corte = var('fecha_corte', '2026-07-01 00:00:00') %}
{% set fecha_corte_ts = "TIMESTAMP '" ~ fecha_corte ~ "'" %}
{% set fecha_inicio_28d = "(" ~ fecha_corte_ts ~ " - INTERVAL '28 days')" %}
{% set fecha_inicio_14d = "(" ~ fecha_corte_ts ~ " - INTERVAL '14 days')" %}
{% set fecha_inicio_7d  = "(" ~ fecha_corte_ts ~ " - INTERVAL '7 days')" %}

WITH unioned AS (
    -- Transmetro
    SELECT
        NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '')::bigint::text AS sk_usuario,
        timestamp_local,
        'transmetro'::text AS modo,
        estacion::text AS estacion_id
    FROM {{ ref('silver_transmetro_validado') }}
    WHERE regla_calidad = 'VALIDO'

    UNION ALL
    -- Transurbano
    SELECT
        NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '')::bigint::text AS sk_usuario,
        timestamp_local,
        'transurbano'::text AS modo,
        cod_parada::text AS estacion_id
    FROM {{ ref('silver_transurbano_validado') }}
        WHERE regla_calidad = 'VALIDO'
      AND NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '') IS NOT NULL

    UNION ALL
    -- MetroRiel
    SELECT
        NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '')::bigint::text AS sk_usuario,
        timestamp_origen AS timestamp_local,
        'metroriel'::text AS modo,
        estacion_origen::text AS estacion_id
    FROM {{ ref('silver_metroriel_validado') }}
        WHERE regla_calidad = 'VALIDO'
      AND NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '') IS NOT NULL

    UNION ALL
    -- Aerómetro
    SELECT
        user_hash AS sk_usuario,
        timestamp_local,
        'aerometro'::text AS modo,
        station_code::text AS estacion_id
    FROM {{ ref('silver_aerometro_validado') }}
        WHERE regla_calidad = 'VALIDO'
      AND NULLIF(REGEXP_REPLACE(user_hash, '\D', '', 'g'), '') IS NOT NULL
),

ventana AS (
    SELECT *
    FROM unioned
    WHERE timestamp_local >= {{ fecha_inicio_28d }}
      AND timestamp_local <  {{ fecha_corte_ts }}
),

agregado AS (
    SELECT
        sk_usuario,
        COUNT(*)                                                          AS n_abordajes_28d,
        COUNT(DISTINCT DATE(timestamp_local))                             AS n_dias_activos_28d,
        COUNT(DISTINCT modo)                                              AS n_modos_usados_28d,
        AVG(CASE WHEN EXTRACT(ISODOW FROM timestamp_local) IN (6, 7)
                 THEN 1.0 ELSE 0.0 END)                                   AS pct_fin_semana_28d,
        COUNT(*) FILTER (WHERE timestamp_local >= {{ fecha_inicio_14d }}) AS n_abordajes_14d,
        COUNT(*) FILTER (WHERE timestamp_local >= {{ fecha_inicio_7d }})  AS n_abordajes_7d,
        COUNT(DISTINCT estacion_id)                                       AS n_estaciones_distintas_28d,
        AVG(CASE WHEN EXTRACT(HOUR FROM timestamp_local) BETWEEN 6 AND 8
                   OR EXTRACT(HOUR FROM timestamp_local) BETWEEN 17 AND 19
                 THEN 1.0 ELSE 0.0 END)                                   AS pct_hora_pico_28d,
        MAX(timestamp_local)                                              AS ultimo_abordaje_ts,
        MIN(timestamp_local)                                              AS primer_abordaje_en_ventana_ts
    FROM ventana
    GROUP BY sk_usuario
),

con_recencia AS (
    SELECT
        a.*,
        EXTRACT(EPOCH FROM ({{ fecha_corte_ts }} - a.ultimo_abordaje_ts)) / 86400.0
            AS recencia_dias_ultimo_abordaje
    FROM agregado a
),

con_antiguedad AS (
    SELECT
        r.*,
        -- dias_desde_alta: desde el primer abordaje histórico (no solo en ventana)
        EXTRACT(EPOCH FROM (
            {{ fecha_corte_ts }} - h.primer_abordaje_historico
        )) / 86400.0 AS dias_desde_alta
    FROM con_recencia r
    LEFT JOIN (
        SELECT sk_usuario, MIN(timestamp_local) AS primer_abordaje_historico
        FROM unioned
        WHERE timestamp_local < {{ fecha_corte_ts }}
        GROUP BY sk_usuario
    ) h ON h.sk_usuario = r.sk_usuario
)

SELECT
    sk_usuario,
    {{ fecha_corte_ts }}                              AS fecha_corte,
    n_abordajes_28d,
    n_dias_activos_28d,
    n_modos_usados_28d,
    ROUND(pct_fin_semana_28d::numeric, 4)             AS pct_fin_semana_28d,
    n_abordajes_14d,
    n_abordajes_7d,
    ROUND(recencia_dias_ultimo_abordaje::numeric, 2)  AS recencia_dias_ultimo_abordaje,
    n_estaciones_distintas_28d,
    ROUND(pct_hora_pico_28d::numeric, 4)              AS pct_hora_pico_28d,
    ROUND(dias_desde_alta::numeric, 2)                AS dias_desde_alta
FROM con_antiguedad