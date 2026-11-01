{{ config(materialized='table') }}

{#
  Tabla de consumo para 1.4 y 2.x.
  - Features + etiqueta + flag de población, por fecha de corte.
  - Para corte C, etiqueta = NULL (se filtra en el consumo).
  - Columnas: sk_usuario, fecha_corte, <10 features>, etiqueta, en_poblacion.
#}

{% set fecha_corte = var('fecha_corte', '2026-07-01 00:00:00') %}
{% set fecha_corte_ts = "TIMESTAMP '" ~ fecha_corte ~ "'" %}

WITH f AS (
    SELECT * FROM {{ ref('p2_features_por_corte') }}
),
e AS (
    SELECT sk_usuario, etiqueta FROM {{ ref('p2_etiquetas_por_corte') }}
),
p AS (
    SELECT sk_usuario FROM {{ ref('p2_poblacion_por_corte') }}
)

SELECT
    f.sk_usuario,
    f.fecha_corte,
    f.n_abordajes_28d,
    f.n_dias_activos_28d,
    f.n_modos_usados_28d,
    f.pct_fin_semana_28d,
    f.n_abordajes_14d,
    f.n_abordajes_7d,
    f.recencia_dias_ultimo_abordaje,
    f.n_estaciones_distintas_28d,
    f.pct_hora_pico_28d,
    f.dias_desde_alta,
    e.etiqueta,
    CASE WHEN p.sk_usuario IS NOT NULL THEN TRUE ELSE FALSE END AS en_poblacion
FROM f
LEFT JOIN e ON e.sk_usuario = f.sk_usuario
LEFT JOIN p ON p.sk_usuario = f.sk_usuario