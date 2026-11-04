# Fase 2.1 — Ensemble (HGB) y sesgo-varianza

## Configuración

- Modelo: `HistGradientBoostingClassifier` con hiperparámetros base.
- Folds: `StratifiedKFold(n_splits=5, shuffle=True, random_state=42)` (los MISMOS de 1.4).
- Importancia por permutación con `scoring='roc_auc'`, `n_repeats=10`, calculada **en validación**.
- Corte B **NO** se tocó.

## Tabla 1: HGB — train vs validación por fold

| Fold | n_train | n_val | ROC-AUC train | ROC-AUC val | Δ (train − val) |
|---|---|---|---|---|---|
| 1 | 46,880 | 11,720 | 0.8742 | 0.8556 | +0.0186 |
| 2 | 46,880 | 11,720 | 0.8732 | 0.8546 | +0.0186 |
| 3 | 46,880 | 11,720 | 0.8698 | 0.8720 | -0.0022 |
| 4 | 46,880 | 11,720 | 0.8722 | 0.8612 | +0.0110 |
| 5 | 46,880 | 11,720 | 0.8757 | 0.8559 | +0.0198 |

**Resumen:**

- ROC-AUC val (media ± std): **0.8599 ± 0.0072**
- ROC-AUC train (media ± std): 0.8730 ± 0.0022
- Δ promedio (train − val): +0.0131

## Comparación HGB vs L2 (modelo final de 1.4)

| Modelo | ROC-AUC val (media ± std) | PR-AUC val (media ± std) |
|---|---|---|
| L2 λ=100 (1.4) | 0.8488 ± 0.0068 | 0.4762 ± 0.0093 |
| **HGB** | **0.8599 ± 0.0072** | 0.4908 ± 0.0087 |

## Tabla 2: Importancia por permutación (en validación)

| Feature | Importancia media | Std entre folds |
|---|---|---|
| `recencia_dias_ultimo_abordaje` | +0.2524 | 0.0108 |
| `n_abordajes_14d` | +0.0095 | 0.0036 |
| `n_abordajes_28d` | +0.0057 | 0.0012 |
| `dias_desde_alta` | +0.0028 | 0.0007 |
| `n_dias_activos_28d` | +0.0024 | 0.0006 |
| `n_abordajes_7d` | +0.0007 | 0.0004 |
| `n_estaciones_distintas_28d` | +0.0006 | 0.0003 |
| `n_modos_usados_28d` | +0.0003 | 0.0003 |
| `pct_fin_semana_28d` | +0.0003 | 0.0003 |
| `pct_hora_pico_28d` | +0.0003 | 0.0004 |

**Interpretación:** la importancia mide cuánto cae la ROC-AUC de validación cuando se permuta esa feature. Valores **positivos** = la feature aporta. Valores **negativos o ~0** = la feature es ruido o redundante para el modelo.

## Sesgo-varianza

- **Δ (train − val) promedio:** +0.0131
- **Diagnóstico:** no sobreajuste.
  El modelo generaliza bien (Δ < 0.02).

## Decisión justificada

**Modelo elegido para 2.2: `HGB`**

HGB supera a L2 por +0.0111 en ROC-AUC de validación. Se lleva HGB a 2.2.

HGB captura interacciones no lineales que L2 no puede modelar. Se documenta la importancia por permutación como contraparte interpretativa.

## Nota: corte B NO se tocó

Por el rubric 2.1, el corte B sigue intocable. El test único se hace en 2.2.
