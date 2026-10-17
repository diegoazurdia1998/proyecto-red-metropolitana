# Evidencia de Idempotencia de Flujo (Subfase 1.5)

| tabla                         |   conteo_corrida_1 |   conteo_corrida_2 |   diferencia |
|:------------------------------|-------------------:|-------------------:|-------------:|
| silver.cuarentena             |               9724 |               9724 |            0 |
| silver.snap_padron_transmetro |              20149 |              20149 |            0 |
| gold.dim_usuario              |              63782 |              63782 |            0 |
| gold.dim_tiempo               |               2564 |               2564 |            0 |
| gold.dim_estacion_parada      |                468 |                468 |            0 |
| gold.fact_abordajes           |            1688942 |            1688942 |            0 |

**Veredicto:** Idempotente (Cero duplicados)
