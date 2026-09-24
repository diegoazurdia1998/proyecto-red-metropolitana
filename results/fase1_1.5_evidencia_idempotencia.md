# Evidencia de Idempotencia de Flujo (Subfase 1.5)

| tabla                         |   conteo_corrida_1 |   conteo_corrida_2 |   diferencia |
|:------------------------------|-------------------:|-------------------:|-------------:|
| silver.cuarentena             |               5538 |               5538 |            0 |
| silver.snap_padron_transmetro |              20159 |              20159 |            0 |
| gold.dim_usuario              |              94097 |              94097 |            0 |
| gold.dim_tiempo               |               1488 |               1488 |            0 |
| gold.fact_abordajes           |            2055233 |            2055233 |            0 |
| gold.fact_viajes_metroriel    |             299100 |             299100 |            0 |

**Veredicto:** Idempotente (Cero duplicados)
