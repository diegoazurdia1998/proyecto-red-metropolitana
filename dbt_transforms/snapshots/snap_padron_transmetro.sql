{% snapshot snap_padron_transmetro %}

{{
    config(
      target_schema='silver',
      unique_key='tarjeta',
      strategy='timestamp',
      updated_at='fecha_actualizacion',
      invalidate_hard_deletes=False
    )
}}

SELECT
    tarjeta,
    perfil,
    zona_residencia,
    estado,
    es_activa,
    fecha_actualizacion
FROM {{ source('staging', 'padron_transmetro_actual') }}

{% endsnapshot %}