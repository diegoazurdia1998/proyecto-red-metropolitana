# Fase 2.2 — Umbral y test único en B

_Fecha: 2026-11-06T21:43:31 | Commit: `a830ed73c061` | Modelo: HGB_

## Derivación del umbral

**Escenario de costos (enunciado):**
- Costo de contactar: **Q5.00**
- Tasa de retención: **25%**
- Valor de un retenido: **Q60.00**

**Umbral teórico (cuenta completa):**

```
p* = costo_contacto / (valor_retenido × tasa_retencion)
p* = 5.00 / (60.00 × 0.25)
p* = 5.00 / 15.00
p* = 0.3333
```

**Umbral empírico (barrido en A, 0.01–0.99):** `0.3700`

**Umbral elegido:** `0.3333` (= teórico)

## Evaluación única en B

| Métrica | Valor |
|---|---|
| ROC-AUC | **0.8482** |
| PR-AUC | 0.3896 |
| Precisión | 0.4098 |
| Recall | 0.4815 |
| F1 | 0.4428 |
| Contactados | 7,320 |
| TP contactados | 3,000 |
| Retenidos esperados | 750 |
| Beneficio neto | **Q8,400.00** |

**Matriz de confusión en B:**

| | Pred=0 | Pred=1 |
|---|---|---|
| Real=0 | 46,906 | 4,320 |
| Real=1 | 3,231 | 3,000 |

## Top 5 umbrales (por beneficio neto en A)

| Umbral | Beneficio (A) | Contactados (A) | TP (A) |
|---|---|---|---|
| 0.37 | Q18,785.00 | 6,056 | 3,271 |

| 0.36 | Q18,765.00 | 6,276 | 3,343 |

| 0.38 | Q18,725.00 | 5,756 | 3,167 |

| 0.30 | Q18,685.00 | 8,470 | 4,069 |

| 0.31 | Q18,665.00 | 8,108 | 3,947 |

## Nota: B se evaluó UNA SOLA VEZ

Por el rubric 2.2, el test en B es único. No se reentrenó ni re-evaluó. Si se necesita re-evaluar, borrar manualmente `p2_features_B.parquet` y `p2_fase2_2.2_bitacora.csv`.
