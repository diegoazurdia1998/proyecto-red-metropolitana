{{ config(materialized='table') }}

{#
  Tabla de resumen por corte (rubric 1.2).
  - Una fila por corte (A, B, C) en una sola corrida de dbt.
  - Recalcula población y etiqueta desde Silver para cada corte (autocontenido).
  - Consistencia garantizada por el script p2_exportar_resumen_1_2.py
    (verifica contra p2_features_etiqueta_por_corte).
  - Corte C: positivos_churn = NULL y tasa_churn_pct = NULL (etiqueta oculta).
#}

{% set cortes = [
    {'nombre': 'A', 'fecha': '2026-07-01 00:00:00', 'etiqueta_oculta': false},
    {'nombre': 'B', 'fecha': '2026-07-31 00:00:00', 'etiqueta_oculta': false},
    {'nombre': 'C', 'fecha': '2026-08-30 00:00:00', 'etiqueta_oculta': true}
] %}

WITH unioned AS (
    -- Unión de Silver (solo VALIDO), con sk_usuario canónico
    SELECT
        NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '')::bigint::text AS sk_usuario,
        timestamp_local
    FROM {{ ref('silver_transmetro_validado') }}
    WHERE regla_calidad = 'VALIDO'
      AND NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '') IS NOT NULL

    UNION ALL
    SELECT
        NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '')::bigint::text AS sk_usuario,
        timestamp_local
    FROM {{ ref('silver_transurbano_validado') }}
    WHERE regla_calidad = 'VALIDO'
      AND NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '') IS NOT NULL

    UNION ALL
    SELECT
        NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '')::bigint::text AS sk_usuario,
        timestamp_origen AS timestamp_local
    FROM {{ ref('silver_metroriel_validado') }}
    WHERE regla_calidad = 'VALIDO'
      AND NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '') IS NOT NULL

    UNION ALL
    SELECT
        user_hash AS sk_usuario,
        timestamp_local
    FROM {{ ref('silver_aerometro_validado') }}
    WHERE regla_calidad = 'VALIDO'
      AND user_hash IS NOT NULL
),

padron_estado AS (
    SELECT
        NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '')::bigint::text AS sk_usuario,
        es_activa
    FROM {{ source('staging', 'padron_transmetro_actual') }}
    WHERE tarjeta IS NOT NULL
      AND NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '') IS NOT NULL
),

{% for c in cortes %}
bloque_{{ c.nombre }} AS (
    WITH
    features AS (
        SELECT
            sk_usuario,
            COUNT(*) AS n_abordajes_28d
        FROM unioned
        WHERE timestamp_local >= TIMESTAMP '{{ c.fecha }}' - INTERVAL '28 days'
          AND timestamp_local <  TIMESTAMP '{{ c.fecha }}'
        GROUP BY sk_usuario
        HAVING COUNT(*) >= 2
    ),
    poblacion AS (
        SELECT
            f.sk_usuario
        FROM features f
        LEFT JOIN padron_estado p ON p.sk_usuario = f.sk_usuario
        WHERE p.sk_usuario IS NULL
           OR p.es_activa = TRUE
    ),
    {% if c.etiqueta_oculta %}
    etiquetas AS (
        SELECT sk_usuario, NULL::int AS etiqueta FROM poblacion
    ),
    {% else %}
    etiquetas AS (
        SELECT
            p.sk_usuario,
            CASE WHEN COALESCE(cnt.n_abordajes_30d, 0) = 0 THEN 1 ELSE 0 END AS etiqueta
        FROM poblacion p
        LEFT JOIN (
            SELECT sk_usuario, COUNT(*) AS n_abordajes_30d
            FROM unioned
            WHERE timestamp_local >= TIMESTAMP '{{ c.fecha }}'
              AND timestamp_local <  TIMESTAMP '{{ c.fecha }}' + INTERVAL '30 days'
            GROUP BY sk_usuario
        ) cnt ON cnt.sk_usuario = p.sk_usuario
    ),
    {% endif %}
    resumen AS (
        SELECT
            '{{ c.nombre }}'::text                          AS corte,
            TIMESTAMP '{{ c.fecha }}'                       AS fecha_corte,
            (SELECT COUNT(*) FROM features)                 AS poblacion_total,
            (SELECT COUNT(*) FROM poblacion)                AS poblacion_elegible,
            {% if c.etiqueta_oculta %}
            NULL::bigint                                    AS positivos_churn,
            NULL::numeric                                   AS tasa_churn_pct
            {% else %}
            (SELECT COUNT(*) FROM etiquetas WHERE etiqueta = 1)                AS positivos_churn,
            ROUND(
                100.0 * (SELECT COUNT(*) FROM etiquetas WHERE etiqueta = 1)
                / NULLIF((SELECT COUNT(*) FROM etiquetas), 0),
                2
            )                                                                   AS tasa_churn_pct
            {% endif %}
    )
    SELECT * FROM resumen
)
{% if not loop.last %},{% endif %}
{% endfor %}

SELECT * FROM bloque_A
UNION ALL
SELECT * FROM bloque_B
UNION ALL
SELECT * FROM bloque_C
ORDER BY fecha_corte