{{ config(materialized='table') }}

{#
  Etiqueta de churn por fecha de corte (rubric 1.3 + 1.2).
  - 1 si NO hay NINGÚN abordaje válido en los 30 días siguientes a t.
  - 0 si hay al menos uno.
  - Para corte C (2026-08-30) devuelve etiqueta NULL (etiqueta OCULTA, D10).
#}

{% set fecha_corte = var('fecha_corte', '2026-07-01 00:00:00') %}
{% set fecha_corte_ts = "TIMESTAMP '" ~ fecha_corte ~ "'" %}
{% set fecha_fin_30d = "(" ~ fecha_corte_ts ~ " + INTERVAL '30 days')" %}

{% set es_corte_c = (fecha_corte | string).startswith('2026-08-30') %}

WITH unioned AS (
    SELECT
        REGEXP_REPLACE(tarjeta, '\D', '', 'g')::bigint::text AS sk_usuario,
        timestamp_local
    FROM {{ ref('silver_transmetro_validado') }}
    WHERE regla_calidad = 'VALIDO'

    UNION ALL
    SELECT
        REGEXP_REPLACE(num_tarjeta, '\D', '', 'g')::bigint::text AS sk_usuario,
        timestamp_local
    FROM {{ ref('silver_transurbano_validado') }}
    WHERE regla_calidad = 'VALIDO'

    UNION ALL
    SELECT
        REGEXP_REPLACE(num_tarjeta, '\D', '', 'g')::bigint::text AS sk_usuario,
        timestamp_origen AS timestamp_local
    FROM {{ ref('silver_metroriel_validado') }}
    WHERE regla_calidad = 'VALIDO'

    UNION ALL
    SELECT
        user_hash AS sk_usuario,
        timestamp_local
    FROM {{ ref('silver_aerometro_validado') }}
    WHERE regla_calidad = 'VALIDO'
),

conteo_30d AS (
    SELECT sk_usuario, COUNT(*) AS n_abordajes_30d
    FROM unioned
    WHERE timestamp_local >= {{ fecha_corte_ts }}
      AND timestamp_local <  {{ fecha_fin_30d }}
    GROUP BY sk_usuario
),

universo AS (
    -- Universo de evaluación: los que tienen features en esta fecha de corte
    SELECT sk_usuario FROM {{ ref('p2_features_por_corte') }}
)

SELECT
    u.sk_usuario,
    {{ fecha_corte_ts }} AS fecha_corte,
    {% if es_corte_c %}
        NULL::int AS etiqueta
    {% else %}
        CASE WHEN COALESCE(c.n_abordajes_30d, 0) = 0 THEN 1 ELSE 0 END AS etiqueta
    {% endif %}
FROM universo u
LEFT JOIN conteo_30d c ON c.sk_usuario = u.sk_usuario