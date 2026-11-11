# Ficha del modelo — `p2_hgb_v1.0.0`

## Identificación

- **Nombre:** HGB Churn AMT

- **Versión:** `v1.0.0`

- **Archivo:** `modelos_entrenados/p2_hgb_v1.0.0.pkl`

- **Tipo:** `sklearn.ensemble.HistGradientBoostingClassifier`

- **Fecha de entrenamiento:** 2026-11-03

- **Autor:** Diego Azurdia (2528119)

- **Commit de referencia:** `efcfb004eb19` (fase-2.1)

## Uso previsto

Clasificación binaria de churn para la campaña de retención de la AMT.

Inferencia mediante:

```bash

python -m scripts.predecir --fecha-corte YYYY-MM-DD

```

## Entrenamiento

- **Corte:** A (`2026-07-01 00:00:00`)

- **Ventana de features:** 28 días previos al corte

- **Ventana de etiqueta:** 30 días siguientes al corte

- **Población:** ≥2 abordajes en 28d y sin DELETE CDC antes del corte

- **N train:** 58,600 usuarios (7,486 positivos; 12.77%)

## Features de entrada (10)

1. `n_abordajes_28d` — abordajes en 28d

2. `n_dias_activos_28d` — días activos en 28d

3. `n_modos_usados_28d` — modos distintos usados

4. `pct_fin_semana_28d` — fracción de abordajes en sáb/dom

5. `n_abordajes_14d` — abordajes en 14d

6. `n_abordajes_7d` — abordajes en 7d

7. `recencia_dias_ultimo_abordaje` — días desde el último abordaje

8. `n_estaciones_distintas_28d` — estaciones distintas en 28d

9. `pct_hora_pico_28d` — fracción de abordajes en hora pico

10. `dias_desde_alta` — días desde el primer abordaje histórico

## Hiperparámetros

```python

HistGradientBoostingClassifier(

    max_iter=200,

    learning_rate=0.05,

    max_depth=5,

    min_samples_leaf=20,

    l2_regularization=1.0,

    random_state=42,

)

```

## Desempeño

| Evaluación | ROC-AUC | PR-AUC |
|------------|---------|---------|
| CV 5-fold en A (2.1) | **0.8599 ± 0.0068** | 0.4770 ± 0.0095 |
| Test único en B (2.2) | **0.8482** | 0.3896 |
| Prueba de invariancia (2.4) | — | — |

## Umbral de decisión

- **Umbral:** `p* = 0.3333`

- **Derivación:** `5 / (60 × 0.25)` (costo contacto / (valor retenido × tasa retención))

- **Contactar si:** **`puntaje > 0.3333`**

- **Justificación:** derivación formal del escenario de costos, no depende de la muestra.

## Métrica de negocio (test B, umbral 0.3333)

- Contactados: 7,320

- TP contactados: 3,000

- Retenidos esperados (25%): 750

- **Beneficio neto: Q8,400**

## Limitaciones

1. Modelo entrenado en A; en producción puede requerir recalibración periódica.

2. Fuertemente dominado por `recencia_dias_ultimo_abordaje` (importancia por permutación 0.25; 26× la segunda feature).

3. No usa segmento KMeans como feature (decisión de 2.3, Q41).

4. Aerómetro usa `user_hash` aislado: sus usuarios no se vinculan a otros modos.

## Reproducibilidad

1. Requiere `scikit-learn==1.7.2`, `joblib`, `pandas`, `psycopg2-binary`, `python-dotenv`.

2. Requiere Postgres con Silver + features del corte pedido.

3. Determinismo verificado (`random_state=42`; predicciones idénticas en corridas repetidas).

## Prueba de invariancia

Ver:

```text

results/p2_fase2_2.4_invariancia.md

```

