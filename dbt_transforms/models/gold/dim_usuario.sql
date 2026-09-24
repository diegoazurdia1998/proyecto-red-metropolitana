{{ config(materialized='table') }}

WITH usuarios_transmetro AS (
    SELECT
        'Transmetro' AS modo,
        tarjeta AS id_origen,
        perfil,
        zona_residencia,
        es_activa
    FROM {{ ref('snap_padron_transmetro') }}
    WHERE dbt_valid_to IS NULL
),

usuarios_transurbano AS (
    SELECT
        'Transurbano' AS modo,
        num_tarjeta AS id_origen,
        'No Declarado' AS perfil,
        'No Definida' AS zona_residencia,
        TRUE AS es_activa
    FROM {{ source('staging', 'catalogo_minimo_transurbano') }}
),

usuarios_metroriel AS (
    SELECT
        'MetroRiel' AS modo,
        card AS id_origen,
        'No Declarado' AS perfil,
        'No Definida' AS zona_residencia,
        TRUE AS es_activa
    FROM {{ source('staging', 'catalogo_minimo_metroriel') }}
),

usuarios_aerometro AS (
    SELECT
        'Aerometro' AS modo,
        user_hash AS id_origen,
        'No Declarado' AS perfil,
        'No Definida' AS zona_residencia,
        TRUE AS es_activa
    FROM {{ source('staging', 'catalogo_minimo_aerometro') }}
),

consolidados AS (
    SELECT * FROM usuarios_transmetro
    UNION ALL
    SELECT * FROM usuarios_transurbano
    UNION ALL
    SELECT * FROM usuarios_metroriel
    UNION ALL
    SELECT * FROM usuarios_aerometro
)

SELECT
    MD5(modo || ':' || id_origen) AS sk_usuario,
    SHA256((modo || ':' || id_origen)::bytea)::varchar AS usuario_pseudonimo,
    modo AS sistema_origen,
    perfil,
    {{ normalizar_zona('zona_residencia') }} AS zona_residencia,
    es_activa
FROM consolidados