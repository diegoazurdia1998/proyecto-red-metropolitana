# Evidencia de Idempotencia (Subfase 1.5)

## Semantica

Reejecutar el pipeline completo desde cero (limpieza total) produce el mismo resultado.

## Conteos por tabla

| tabla                         |   conteo_corrida_1 |   conteo_corrida_2 |   diferencia |
|:------------------------------|-------------------:|-------------------:|-------------:|
| gold.dim_estacion_parada      |                468 |                468 |            0 |
| gold.dim_tiempo               |               3644 |               3644 |            0 |
| gold.dim_usuario              |              87937 |              87937 |            0 |
| gold.fact_abordajes           |            3308556 |            3308556 |            0 |
| silver.cuarentena             |              18914 |              18914 |            0 |
| silver.snap_padron_transmetro |              47259 |              47259 |            0 |## Veredicto

**IDEMPOTENTE**
