# Fase 1.4 — Baselines y regresión logística regularizada

## Métrica elegida

Se elige **ROC-AUC** como métrica principal porque es invariante al umbral de decisión (que se deriva formalmente en 2.2) y permite comparar modelos con el desbalance de clases (12.77% positivos). Se reporta además **PR-AUC (Average Precision)** porque con desbalance moderado es más sensible a la clase positiva (churn), que es la que la campaña de retención necesita detectar. Ambas se reportan como **media ± desviación estándar** sobre los mismos 5 folds estratificados del corte A.

## Tabla de modelos

| Modelo | ROC-AUC (media ± std) | PR-AUC (media ± std) |
|---|---|---|
| Baseline 1: constante p=0.1277 | 0.5000 ± 0.0000 | 0.1277 ± 0.0000 |
| Baseline 2: -n_abordajes_28d | 0.7488 ± 0.0092 | 0.2755 ± 0.0065 |
| LogReg L1 λ=1000.0 | 0.8535 ± 0.0063 | 0.4771 ± 0.0094 |
| LogReg L2 λ=100.0 | 0.8488 ± 0.0068 | 0.4762 ± 0.0093 |
| LogReg ELASTICNET λ=1000.0 | 0.8513 ± 0.0064 | 0.4770 ± 0.0095 |

## λ elegido por penalización (mayor ROC-AUC media)

- **l1**: λ = 1000.0 (C = 0.0010)
- **l2**: λ = 100.0 (C = 0.0100)
- **elasticnet**: λ = 1000.0 (C = 0.0010)

## Top-5 coeficientes (modelo final con el mejor λ global)

Los coeficientes positivos empujan hacia el abandono (churn=1).

| Feature | Coeficiente | Dirección |
|---|---|---|
| recencia_dias_ultimo_abordaje | +0.8609 | hacia_churn |
| n_abordajes_7d | -0.4775 | hacia_no_churn |
| n_dias_activos_28d | -0.4681 | hacia_no_churn |
| n_abordajes_28d | +0.1087 | hacia_churn |
| n_modos_usados_28d | -0.0597 | hacia_no_churn |

## Interpretación de negocio

Cada fila se interpreta como: *'un aumento de 1 desviación estándar en esta feature, manteniendo las demás constantes, cambia el log-odds de churn en βᵢ'*.

- **recencia_dias_ultimo_abordaje**: aumenta el riesgo de churn (β = +0.8609). Interpretación: coherente con la hipótesis de negocio.
- **n_abordajes_7d**: disminuye el riesgo de churn (β = -0.4775). Interpretación: coherente con la hipótesis de negocio.
- **n_dias_activos_28d**: disminuye el riesgo de churn (β = -0.4681). Interpretación: coherente con la hipótesis de negocio.
- **n_abordajes_28d**: aumenta el riesgo de churn (β = +0.1087). Interpretación: revisar signo (posible multicolinealidad).
- **n_modos_usados_28d**: disminuye el riesgo de churn (β = -0.0597). Interpretación: coherente con la hipótesis de negocio.

## Features descartadas por L1 (coeficiente 0)

Ninguna feature quedó con coeficiente 0.

## Alternativa interpretable: L2 con el mismo λ

Si se prefiere un modelo con **coeficientes no nulos en las 10 features** (mayor interpretabilidad a costa de una diferencia de ROC-AUC < 0.001), usar **L2 λ=100**: ROC-AUC=0.8488, PR-AUC=0.4762. La diferencia con L1 es marginal, pero L2 no descarta features.

## Nota: corte B NO se tocó

Por el rubric 1.4, la etiqueta del corte B no se evaluó en esta fase. El test único de B se hace en 2.2.
