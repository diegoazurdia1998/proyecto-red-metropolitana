# Reporte de Calidad de Datos y Cuarentena (Subfase 1.3)

- **Total Registros en Cuarentena:** 18914

## Distribucion de Fallas por Fuente y Regla

| fuente | regla_violada | registros_rechazados |
|---|---|---|
| MetroRiel | VIAJE_SIN_SALIDA | 7099 |
| Transmetro | DUPLICADO_TORNIQUETE | 2102 |
| Transurbano | PARADA_NULA | 8076 |
| Transurbano | FECHA_FUTURA | 1637 |


---
### Criterios de Aislamiento Aplicados:
- **DUPLICADO_TORNIQUETE:** Marcaciones consecutivas en ventana <= 120 segundos.
- **PARADA_NULA:** Codigo de estacion/parada vacio o nulo.
- **FECHA_FUTURA:** Timestamps posteriores al tiempo actual.
- **VIAJE_SIN_SALIDA:** Registros sin evento de salida en sistemas cerrados (MetroRiel).
