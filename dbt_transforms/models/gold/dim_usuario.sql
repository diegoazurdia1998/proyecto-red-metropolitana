{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

WITH usuarios_transmetro AS (
    SELECT
        'Transmetro' AS modo,
        -- Extraer sólo dígitos numéricos de TC-00012345 -> 12345
        NULLIF(REGEXP_REPLACE(tarjeta, '\D', '', 'g'), '')::bigint::text AS id_canonico,
        perfil,
        zona_residencia,
        es_activa,
        1 AS prioridad_calidad
    FROM {{ ref('snap_padron_transmetro') }}
    WHERE dbt_valid_to IS NULL
),

usuarios_transurbano AS (
    SELECT
        'Transurbano' AS modo,
        -- Quitar ceros a la izquierda de 0000012345 -> 12345
        NULLIF(REGEXP_REPLACE(num_tarjeta, '\D', '', 'g'), '')::bigint::text AS id_canonico,
        'No Declarado' AS perfil,
        'No Definida' AS zona_residencia,
        TRUE AS es_activa,
        2 AS prioridad_calidad
    FROM {{ source('staging', 'catalogo_minimo_transurbano') }}
),

usuarios_metroriel AS (
    SELECT
        'MetroRiel' AS modo,
        -- Extraer sólo dígitos numéricos de MR0012345 -> 12345
        NULLIF(REGEXP_REPLACE(card, '\D', '', 'g'), '')::bigint::text AS id_canonico,
        'No Declarado' AS perfil,
        'No Definida' AS zona_residencia,
        TRUE AS es_activa,
        3 AS prioridad_calidad
    FROM {{ source('staging', 'catalogo_minimo_metroriel') }}
),

usuarios_aerometro AS (
    SELECT
        'Aerometro' AS modo,
        -- Conservar el hash de 12 caracteres original
        user_hash AS id_canonico,
        'No Declarado' AS perfil,
        'No Definida' AS zona_residencia,
        TRUE AS es_activa,
        4 AS prioridad_calidad
    FROM {{ source('staging', 'catalogo_minimo_aerometro') }}
),

consolidados AS (
    SELECT * FROM usuarios_transmetro WHERE id_canonico IS NOT NULL
    UNION ALL
    SELECT * FROM usuarios_transurbano WHERE id_canonico IS NOT NULL
    UNION ALL
    SELECT * FROM usuarios_metroriel WHERE id_canonico IS NOT NULL
    UNION ALL
    SELECT * FROM usuarios_aerometro WHERE id_canonico IS NOT NULL
),

usuarios_unificados AS (
    SELECT
        id_canonico,
        perfil,
        zona_residencia,
        es_activa,
        modo,
        -- Priorizar atributos del padrón oficial (Transmetro) si el usuario coincide
        ROW_NUMBER() OVER (
            PARTITION BY id_canonico
            ORDER BY prioridad_calidad ASC
        ) AS rnk
    FROM consolidados
)

SELECT
    MD5(id_canonico) AS sk_usuario,
    SHA256(id_canonico::bytea)::varchar AS usuario_pseudonimo,
    modo AS sistema_origen_registro,
    perfil,
    {{ normalizar_zona('zona_residencia') }} AS zona_residencia,
    es_activa
FROM usuarios_unificados
WHERE rnk = 1