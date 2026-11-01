# Fase 1.2 — Resumen por corte: población, positivos y tasa de churn

_Generado: 2026-10-30T03:04:28_

## Tabla 1: Corte / Población / Positivos / Tasa (cuenta completa)

| corte   | fecha_corte         |   poblacion_total |   poblacion_elegible |   positivos_churn |   tasa_churn_pct |
|:--------|:--------------------|------------------:|---------------------:|------------------:|-----------------:|
| A       | 2026-07-01 00:00:00 |             60892 |                58600 |              7486 |            12.77 |
| B       | 2026-07-31 00:00:00 |             59306 |                57457 |              6231 |            10.84 |
| C       | 2026-08-30 00:00:00 |             58754 |                58158 |               nan |           nan    |

## Cuenta completa (paso a paso)

### Corte A (2026-07-01 00:00:00)

- **Población total (con ≥2 abordajes en 28d):** 60,892
- **Población elegible (sin DELETE CDC previo):** 58,600
- **Positivos (churn):** 7,486
- **Tasa de churn:** 12.77%

### Corte B (2026-07-31 00:00:00)

- **Población total (con ≥2 abordajes en 28d):** 59,306
- **Población elegible (sin DELETE CDC previo):** 57,457
- **Positivos (churn):** 6,231
- **Tasa de churn:** 10.84%

### Corte C (2026-08-30 00:00:00)

- **Población total (con ≥2 abordajes en 28d):** 58,754
- **Población elegible (sin DELETE CDC previo):** 58,158
- **Positivos (churn):** NULL (etiqueta oculta por diseño)
- **Tasa de churn:** NULL

## Notas metodológicas

- **Población:** unidades con ≥2 abordajes válidos en los 28 días previos al corte, y sin un DELETE del CDC aplicado antes del corte.
- **Etiqueta:** 1 si la unidad NO tiene ningún abordaje válido (en ningún modo) en los 30 días siguientes al corte; 0 si tiene al menos uno.
- **Corte C:** etiqueta oculta por diseño del proyecto (no se evalúa, se usa para uso real / invariancia).
- **Unidad:** persona (`sk_usuario` canónico). Ver `p2_fase1_1.2_costo_unidad.md`.

## ✅ Salvaguarda: consistencia verificada con `p2_features_etiqueta_por_corte`
