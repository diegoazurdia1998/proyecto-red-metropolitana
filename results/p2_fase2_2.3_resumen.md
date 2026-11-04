# Fase 2.3 — KMeans + PCA + segmentos y retención

## Configuración

- Features de clustering (5):
  - `n_abordajes_28d` — Abordajes en 28d
  - `n_dias_activos_28d` — Días activos en 28d
  - `n_modos_usados_28d` — Modos usados
  - `recencia_dias_ultimo_abordaje` — Días desde último abordaje
  - `n_estaciones_distintas_28d` — Estaciones distintas en 28d
- Estandarización: `StandardScaler` (media 0, std 1).
- Barrido `k ∈ {2..8}`.
- **k elegido: 2** (mayor silueta).
- Segmentos NO se usan como feature del modelo (Q41).

## Tabla 1: Barrido de k

| k | Inercia (WCSS) | Silueta |
|---|---|---|
| **2** | 195,187.9 | 0.4108 |
| 3 | 151,719.9 | 0.3220 |
| 4 | 115,474.7 | 0.3823 |
| 5 | 89,144.3 | 0.3975 |
| 6 | 76,871.9 | 0.3809 |
| 7 | 66,580.0 | 0.3921 |
| 8 | 61,234.0 | 0.3508 |

## Tabla 2: Perfil de segmentos (medianas + tamaño + tasa de abandono)

| Segmento | n | % pob. | Abordajes 28d | Días activos | Modos | Recencia (d) | Estaciones | Tasa abandono |
|---|---|---|---|---|---|---|---|---|
| 0 | 44,936 | 76.7% | 7.0 | 6.0 | 1.0 | 3.7 | 3.0 | 15.98% |
| 1 | 13,664 | 23.3% | 33.0 | 17.0 | 2.0 | 0.7 | 11.0 | 2.22% |

## Tabla 3: PCA 2D — varianza explicada

- PC1: **57.50%**
- PC2: **17.83%**
- Total (2D): **75.34%**

El gráfico PCA 2D muestra la separación visual de los segmentos. La varianza explicada indica cuánta información retienen los dos componentes.

## Tabla 4: Acciones de retención por segmento

La clasificación de cada segmento es heurística, basada en las medianas.

| Segmento | Tipo | Acción sugerida |
|---|---|---|
| 0 | `inactivos_en_riesgo` | Contacto personal de retención con oferta de valor (descuento significativo). |
| 1 | `intensivos_regulares` | Programa de lealtad con puntos por viaje + upgrades ocasionales. |

## Notas

- Los segmentos se calculan sobre el corte A (Q38).
- La tasa de abandono por segmento se calcula como `mean(etiqueta)` en A.
- Los segmentos son una capa de análisis post-modelo; **no se usan como feature** (Q41), por lo que no violan el test único en B ni requieren recalcular la CV.
- Archivo de asignación usuario→segmento: `results/p2_fase2_2.3_segmentos.csv`.
