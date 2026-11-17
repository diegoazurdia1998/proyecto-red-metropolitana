# Proyecto 2 — ¿Quién deja de usar la red? (AMT)

Clasificador binario de churn para la red de transporte público del área metropolitana de Guatemala (Transmetro, Transurbano, MetroRiel y Aerómetro).

Predice la probabilidad de que un usuario abandone la red en los 30 días siguientes a una fecha de corte, para priorizar una campaña de retención con presupuesto limitado.

**Entregable:** video de defensa + repositorio congelado en el tag `entrega-final`.

La documentación técnica completa está en `Entregable - Proyecto 2.pdf` y en `Respuestas.md`.

---

## Inicio rápido

```bash
# 1. Clonar el repositorio
git clone https://github.com/diegoazurdia1998/proyecto-red-metropolitana
cd proyecto-red-metropolitana

# 2. Configurar el entorno virtual
python -m venv venv
source venv/bin/activate          # Linux/macOS
.\venv\Scripts\Activate.ps1       # Windows PowerShell

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno
cp .env.example .env
# Editar .env con credenciales de PostgreSQL

# 5. Levantar la infraestructura
docker-compose up -d

# 6. Correr el pipeline completo
python -m scripts.orchestration.pipeline_flow

# 7. Generar predicciones del corte C
python -m scripts.predecir --fecha-corte 2026-08-30 --datos scripts/datos_red
```

---

## Componentes principales

### Pipeline de datos (Prefect + dbt + PostgreSQL)

`scripts/orchestration/pipeline_flow.py` orquesta:

1. Ingesta batch y streaming a Bronze (Parquet).
2. Carga a Staging efímero.
3. Snapshots y modelos dbt (Silver, Gold, Features).
4. Cuarentena y métricas de calidad.
5. Auditoría de ingesta (9 archivos origen vs Bronze).
6. Subfases P2: 1.4 (baselines + LogReg), 2.1 (HGB), 2.2 (umbral + test B), 2.3 (KMeans + PCA).

**Duración típica:** 15–25 min.

### Modelo de churn (HGB)

- **Modelo:** `HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05, max_depth=5)`
- **Features:** 10 variables de comportamiento en ventana de 28 días.
- **Métrica:** ROC-AUC = 0.8599 (CV 5-fold, corte A) y 0.8482 (test único en B).
- **Umbral:** 0.3333 (derivado de Q5 / (Q60 × 25%)).
- **Modelo guardado:** `modelos_entrenados/p2_hgb_v1.0.0.pkl`.
- **Ficha técnica:** `modelos_entrenados/p2_hgb_v1.0.0_ficha.md`.

---

## Predicción (CLI)

```bash
python -m scripts.predecir --fecha-corte 2026-08-30 --datos scripts/P2_datos_red_defensa_120dias
```

**Salida:** `results/p2_predicciones_20260830.csv` con columnas:

```text
unidad_id, tarjetas, puntaje, contactar
```

**Modos:**

- `--datos <carpeta>`: procesa los CSV crudos y filtra por fecha de corte.
- Sin `--datos`: lee features desde PostgreSQL (requiere `dbt run` previo).

**Determinismo:** dos corridas con la misma entrada producen el mismo CSV bit a bit (verificado con `fc.exe`).

---

## Prueba de invariancia temporal

```bash
python -m scripts.p2_prueba_invariancia \
    --datos-90 scripts/datos_red \
    --datos-120 scripts/P2_datos_red_defensa_120dias \
    --fecha-corte "2026-08-30 00:00:00"
```

Verifica que el mismo `predecir.py` aplicado sobre 90 días y 120 días produce el **mismo CSV** para el corte C.

**Eso demuestra que no hay fuga de datos futuros.**

Reporte: `results/p2_fase2_2.4_invariancia.md`.

---

## Estructura del repositorio

```text
proyecto-red-metropolitana/
├── bronze/                         # Parquet particionado YYYY/MM/DD
├── dbt_transforms/
│   ├── macros/                     # generate_schema_name, normalizar_zona
│   ├── models/
│   │   ├── gold/                   # dim_*, fact_*
│   │   ├── silver/                 # silver_*_validado
│   │  *├── staging/                # sour*es.yml, st*_*
│   │   └── features/               # p2_features_por_corte, p2_etiquetas, ...
│   ├── snapshots/                  # snap_padron_transmetro
│   └── dbt_project.yml
├── modelos_entrenados/             # p2_hgb_v1.0.0.pkl, p2_kmeans_v1.0.0.pkl
├── results/                        # Evidencia generada por los scripts
├── scripts/
│   ├── datos_red/                  # 9 archivos fuente (90 días)
│   ├── P2_datos_red_defensa_120dias/
│   │                               # 9 archivos fuente (120 días)
│   ├── orchestration/
│   │   ├── pipeline_flow.py        # Flow principal (P1 + P2)
│   │   └── test_idempotencia.py    # Test 1.5 (solo Medallion)
│   ├── predecir.py                 # CLI de inferencia
│   ├── p2_prueba_invariancia.py    # Prueba 90d vs 120d
│   └── ...                         # Scripts P2
├── .env.example
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---


## Datos
- Fuente: 9 archivos sintéticos (CSV + JSONL), que simulan la operación
de 4 modos de transporte público.

- Periodo: 1-jun a 29-ago-2026 (90 días, proyecto original) o
1-jun a 28-sep-2026 (120 días, prueba de invariancia).

- Cortes temporales:
  - A (2026-07-01): entrenamiento + CV.

  - B (2026-07-31): test único.

  - C (2026-08-30): uso real (etiqueta oculta).

---

## Resultados clave

| Métrica | Valor |
|---|---|
| Tasa de churn (corte A) | 12.77% (7,486 / 58,600) |
| ROC-AUC (CV 5-fold, corte A) | 0.8599 ± 0.0068 |
| ROC-AUC (test único, corte B) | 0.8482 |
| Umbral elegido | 0.3333 |
| Beneficio neto en B | Q8,400 |
| Usuarios contactados en C | 7,427 (12.83%) |
| Segmentos KMeans | 2 (silueta 0.4108) |
| Tasa de churn por segmento | 15.98% vs 2.22% |

---

## Documentación completa

- **Informe técnico:** `Entregable - Proyecto 2.pdf`.
- **Ficha del modelo:** `modelos_entrenados/p2_hgb_v1.0.0_ficha.md`.

---

## Requisitos

- Docker + Docker Compose (PostgreSQL 16 + Kafka 7.5 KRaft).
- Python 3.10+.
- dbt Core v1.8+ con `dbt-postgres`.
- Prefect v3.0+.
- `scikit-learn==1.7.2` (versiones 1.8+ duplican el tiempo del solver `saga`).
- `matplotlib`, `pandas`, `joblib`, `psycopg2-binary`, `python-dotenv`.
