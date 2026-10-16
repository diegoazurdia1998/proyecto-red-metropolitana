# Reporte de Calidad de Datos y Cuarentena (Subfase 1.3)

- **Total Registros en Cuarentena:** 576499

## Distribución de Fallas por Fuente y Regla

| fuente      | regla_violada        |   registros_rechazados |
|:------------|:---------------------|-----------------------:|
| Aerometro   | DUPLICADO_TORNIQUETE |                 203567 |
| MetroRiel   | VIAJE_SIN_SALIDA     |                   3589 |
| Transmetro  | DUPLICADO_TORNIQUETE |                 364337 |
| Transurbano | PARADA_NULA          |                   4189 |
| Transurbano | FECHA_FUTURA         |                    814 |
| Transurbano | DUPLICADO_TORNIQUETE |                      3 |

---
### Criterios de Aislamiento Aplicados:
- **DUPLICADO_TORNIQUETE:** Marcaciones consecutivas en ventana <= 120 segundos.
- **PARADA_NULA:** Código de estación/parada vacío o nulo.
- **FECHA_FUTURA:** Timestamps posteriores al tiempo actual.
- **VIAJE_SIN_SALIDA:** Registros sin evento de salida en sistemas cerrados (MetroRiel).
