{{ config(materialized='table') }}

{#
  Población por fecha de corte (rubric 1.2).
  - Unidades con >= 2 abordajes válidos en los 28d previos a t.
  - Y sin DELETE del CDC aplicado antes de t.
  - Aerómetro no está en el padrón: se incluye por defecto (no hay CDC de baja).
#}

{% set fecha_corte = var('fecha_corte', '2026-07-01 00:00:00') %}
{% set fecha_corte_ts = "TIMESTAMP '" ~ fecha_corte ~ "'" %}

WITH features AS (
    SELECT sk_usuario, n_abordajes_28d
    FROM {{ ref('p2_features_por_corte') }}
    WHERE n_abordajes_28d >= 2
),

padron_estado AS (
    SELECT
        NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '')::bigint::text AS sk_usuario,
        es_activa
    FROM {{ source('staging', 'padron_transmetro_actual') }}
    WHERE tarjeta IS NOT NULL
      AND NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '') IS NOT NULL
),

poblacion AS (
    SELECT
        f.sk_usuario,
        {{ fecha_corte_ts }} AS fecha_corte
    FROM features f
    LEFT JOIN padron_estado p ON p.sk_usuario = f.sk_usuario
    WHERE p.sk_usuario IS NULL    -- no está en padrón (aerómetro puro)
       OR p.es_activa = TRUE      -- está en padrón y activo
)

SELECT * FROM poblacion