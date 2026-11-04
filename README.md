# Red Metropolitana de Transporte (AMT)

## Pipeline End-to-End: Ingeniería de Datos + Modelado de Churn

Solución técnica para la integración, control de calidad, historización  
y modelado analítico de los cuatro modos de transporte público del área  
metropolitana de Guatemala: **Transmetro**, **Transurbano**,  
**MetroRiel** y **Aerómetro**.

El proyecto implementa una **Arquitectura Medallion** (Bronze → Staging  
→ Silver → Gold) orquestada con **Prefect**, transformaciones SQL con  
**dbt Core**, almacenamiento en **PostgreSQL**, ingesta de eventos por  
**Apache Kafka** y un Data Lake columnar en **Apache Parquet**.

Sobre esa plataforma se construye el **Proyecto 2**: un clasificador  
binario que predice la probabilidad de que un usuario abandone la red en  
los próximos 30 días (_churn_), con el objetivo de priorizar una campaña  
de retención con presupuesto limitado.

---

## 1. Arquitectura del flujo

```
[Fuentes de datos]
    ├── Transmetro (Batch CSV + Kafka Streaming + CDC log)
    ├── Transurbano (Batch CSV)
    ├── MetroRiel (Batch JSONL con trayectos anidados)
    └── Aerómetro (Kafka Streaming, timestamp UTC, hash)
             |
             v
[Capa Bronze - Data Lake]
    └── Parquet comprimido SNAPPY, partición YYYY/MM/DD
             |
             v
[Capa Staging - PostgreSQL efímero]
    └── Vaciado y reconstrucción en cada corrida (DROP/TRUNCATE)
             |
             v
[Capa Silver - Calidad, SCD2 y limpieza]
    ├── silver.cuarentena (payload JSONB + regla violada)
    ├── Reglas: duplicado torniquete (<= 120 s), nulo, futuro, sin salida
    ├── SCD Tipo 2: snap_padron_transmetro
    └── features.p2_* (población, etiqueta y features por corte A/B/C)
             |
             v
[Capa Gold - Data Warehouse dimensional (Kimball)]
    ├── Dimensiones conformadas: dim_tiempo, dim_usuario, dim_estacion_parada
    └── Hechos: fact_abordajes (unificada), fact_viajes_metroriel (acumulada)
```

### Subfases del Proyecto 2

- **1.4 --- Baselines y regresión logística regularizada:**  
    L1/L2/ElasticNet, CV 5-fold sobre el corte A.
- **2.2 --- Umbral económico:** derivación del umbral (Q5 contacto /  
    Q25 retención / Q60 retenido) y evaluación única sobre el corte B.
- **2.3 --- Segmentación:** KMeans + PCA sobre features del corte A,  
    con perfiles y acciones de retención por segmento.
- **2.4 --- Pendiente:** `predecir.py`, modelo versionado y prueba de  
    invariancia.

---

## 2. Requisitos

- **Docker** y **Docker Compose** (PostgreSQL 16 + Kafka 7.5 KRaft).
- **Python 3.10+** con entorno virtual activado.
- **dbt Core** con `dbt-postgres` (`v1.8+`).
- **Prefect** (`v3.0+`).

---

## 3. Configuración

### 3.1. Clonar el repositorio y preparar el entorno

```
git clone https://github.com/diegoazurdia1998/proyecto-red-metropolitana
cd proyecto-red-metropolitana

python -m venv venv

# Linux/macOS
source venv/bin/activate

# Windows PowerShell
.\venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### 3.2. Configurar las credenciales (`.env`)

Crea un archivo `.env` en la raíz del proyecto con las siguientes  
variables:

```
DB_HOST=localhost
DB_PORT=5432
DB_USER=amt_admin
DB_PASSWORD=
DB_NAME=amt_dw
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

El archivo `.env` está excluido por `.gitignore`. Usa `.env.example`  
como plantilla y evita subir credenciales reales al repositorio.

### 3.3. Levantar la infraestructura

```
docker-compose up -d
docker ps   # Verificar amt_postgres y amt_kafka
```

---

## 4. Ejecución

### 4.1. Comando único — pipeline completo (entregable 1.1)

Corre el pipeline desde un entorno vacío y genera **todos los artefactos de
Fase 1 (1.1, 1.4) y Fase 2 (2.1, 2.2, 2.3)**:

```bash
python scripts/orchestration/pipeline_flow.py
```
**Qué hace, en orden:**

1. Limpieza de `bronze/` + `results/` + reset de offsets/consumer groups Kafka.
    
2. Ingesta batch/CDC → Bronze (Parquet).
    
3. Ingesta streaming (Kafka) → Bronze.
    
4. Carga a Staging (PostgreSQL, efímero).
    
5. dbt snapshot + dbt run (Silver, Gold).
    
6. Cuarentena + métricas de calidad.
    
7. Conteos de idempotencia (16 tablas).
    
8. Auditoría 1.1 (9 archivos origen vs Bronze + tabla de tiempos).
    
9. **Subfases P2:**
    
    - `1.4` — Exportar features A + baselines + LogReg regularizada.
        
    - `2.1` — Ensemble (HGB) + folds + importancia por permutación.
        
    - `2.2` — Exportar features B (una sola vez) + derivación de umbral + test único.
        
    - `2.3` — KMeans + PCA + perfil de segmentos.
        

**Duración típica:** ~15–25 min según hardware.

**Artefactos generados en `results/`:**

|Archivo|Subfase|Contenido|
|---|---|---|
|`p2_tiempos_etapa.csv`|1.1|Tiempo por etapa (10 filas)|
|`p2_fase1_1.1_conteos.csv`|1.1|9 archivos: origen vs Bronze + diferencia + explicación|
|`p2_fase1_1.1_resumen.md`|1.1|Tablas de conteos y tiempos|
|`p2_features_A.parquet`|1.4|Features + etiqueta del corte A|
|`p2_fase1_1.4_tabla_modelos.csv`|1.4|Baselines + L1/L2/ElasticNet (ROC-AUC y PR-AUC, media ± std)|
|`p2_fase1_1.4_metricas_vs_lambda.png`|1.4|ROC-AUC y PR-AUC vs λ|
|`p2_fase1_1.4_top5_coeficientes.csv`|1.4|Top-5 features hacia churn|
|`p2_fase1_1.4_resumen.md`|1.4|Métrica justificada, tabla, λ elegido, interpretación|
|`p2_fase2_2.1_folds.csv`|2.1|Índices de los 5 folds reutilizados|
|`p2_fase2_2.1_importancia_permutacion.csv`|2.1|Top-10 importancia en validación|
|`p2_fase2_2.1_train_val_e_importancia.png`|2.1|Métrica train vs val + importancia|
|`p2_fase2_2.1_resumen.md`|2.1|Sesgo-varianza por modelo + decisión|
|`p2_features_B.parquet`|2.2|Features + etiqueta del corte B (se genera una sola vez)|
|`p2_fase2_2.2_barrido_umbral_A.csv`|2.2|Umbral 0.01–0.99 con beneficio neto|
|`p2_fase2_2.2_bitacora.csv`|2.2|Fecha + commit + métricas de la evaluación única en B|
|`p2_fase2_2.2_resumen.md`|2.2|Derivación de `p* = 5/(60×0.25)`, matriz de confusión, beneficio|
|`p2_fase2_2.3_k_seleccion.{csv,png}`|2.3|Codo + silueta para k∈{2..8}|
|`p2_fase2_2.3_pca_2d.png`|2.3|Segmentos en 2 componentes principales|
|`p2_fase2_2.3_perfil_segmentos.csv`|2.3|Medianas, tamaño y tasa de abandono por segmento|
|`p2_fase2_2.3_resumen.md`|2.3|Tablas + acciones de retención por segmento|
|`modelos_entrenados/p2_hgb_v1.0.0.pkl`|2.2|Modelo HGB final (entrenado con todo A)|
|`modelos_entrenados/p2_kmeans_v1.0.0.pkl`|2.3|KMeans + scaler para asignar C en 2.4|
### 4.2. Predicción sobre el corte C (entregable 2.4)

Una vez que el modelo está entrenado y versionado en  
`modelos_entrenados/p2_hgb_v1.0.0.pkl`, se genera el CSV de predicciones del  
corte C:

bash

python predecir.py --datos scripts/datos_red --fecha-corte 2026-08-30

**Salida:** `results/predicciones_corte_C.csv` con columnas  
`unidad_id, tarjetas, puntaje, contactar` (una fila por unidad de la población  
del corte C).

> El script **no reentrena**: carga el `.pkl` versionado y solo aplica el  
> pipeline de preprocesamiento + modelo. La reproducibilidad está garantizada  
> por el `random_state=42` con el que se entrenó el modelo.

### 4.3. Test de idempotencia (entregable 1.5) — solo bajo demanda

> ⚠️ **Este test ya fue ejecutado y su evidencia está versionada en  
> `results/fase1_1.5_*`. NO se requiere volver a correrlo para la entrega.**

El test valida que el Medallion produce los mismos conteos en dos corridas  
consecutivas desde cero. **No toca el corte B** y **no genera artefactos de P2**  
(corre con `incluir_p2=False`). Si por alguna razón se necesita re-ejecutar  
(por ejemplo, para verificar un cambio en la ingesta):

bash

python scripts/orchestration/test_idempotencia.py

**Artefactos:** `results/fase1_1.5_conteos_corridas.csv`,  
`results/fase1_1.5_evidencia_idempotencia.md`.

> ⚠️ **Orden de ejecución**: el test **borra `results/`** entre sus dos  
> corridas internas. Si se corre **después** del pipeline completo (§4.1),  
> elimina los artefactos de 1.4, 2.1, 2.2 y 2.3, y regenera B (rompe el  
> rubric 2.2: "B se evalúa una sola vez"). Si se corre, hacerlo **antes** del  
> pipeline y volver a correr el pipeline después.

---

## 5. Consultas de verificación

Conéctate a PostgreSQL:

```
docker exec -it amt_postgres psql -U amt_admin -d amt_dw
```

Ejecuta las siguientes consultas dentro de `psql`:

```
-- 1. Cuarentena por fuente y regla
SELECT fuente, regla_violada, COUNT(*) AS total
FROM silver.cuarentena
GROUP BY 1, 2;

-- 2. Padrón vigente (SCD2)
SELECT perfil, es_activa, COUNT(*) AS total
FROM silver.snap_padron_transmetro
WHERE dbt_valid_to IS NULL
GROUP BY 1, 2;

-- 3. Recaudo por modo
SELECT sistema_modo, COUNT(*) AS abordajes, SUM(monto_gtq) AS recaudo_gtq
FROM gold.fact_abordajes
GROUP BY 1;

-- 4. Población, positivos y tasa de churn por corte (P2)
SELECT fecha_corte, poblacion, positivos,
       ROUND(positivos::numeric / NULLIF(poblacion, 0), 4) AS tasa_churn
FROM features.p2_resumen_por_corte
ORDER BY fecha_corte;

-- 5. Bitácora del test único en B
SELECT *
FROM (
    SELECT 'B' AS corte, roc_auc_B, precision_B, recall_B, beneficio_neto_B
    FROM features.p2_bitacora_b
) AS t;
```

---

## 6. Estructura del repositorio

```
proyecto-red-metropolitana/
├── bronze/                         # Parquet particionado YYYY/MM/DD
├── dbt_transforms/
│   ├── macros/                      # generate_schema_name, normalizar_zona
│   ├── models/
│   │   ├── gold/                    # dim_*, fact_*
│   │   ├── silver/                  # silver_*_validado, silver_features_usuario
│   │   └── staging/                 # sources.yml, stg_*
│   ├── snapshots/                   # snap_padron_transmetro
│   └── dbt_project.yml
├── modelos_entrenados/              # Modelos serializados del Proyecto 2
├── results/                         # Evidencia (ver secciones 4.1 y 4.2)
├── scripts/
│   ├── datos_red/                   # 9 archivos fuente
│   └── orchestration/
│       ├── pipeline_flow.py         # Flow principal (P1 + P2)
│       ├── test_idempotencia.py     # Test 1.5 (solo Medallion)
│       ├── p2_auditoria_ingesta_9_archivos.py
│       ├── p2_medir_tiempos.py
│       ├── p2_exportar_features_A.py
│       ├── p2_baselines_logreg.py
│       ├── p2_umbral_test_unico.py
│       ├── p2_kmeans_pca.py
│       └── ...                     # Scripts P1 de ingesta, staging y cuarentena
├── .env                             # Variables locales (no versionar)
├── .env.example                     # Plantilla de variables
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 7. Notas de diseño

### Idempotencia

`test_idempotencia.py` valida el flujo Medallion con dos corridas desde  
cero. Las subfases del Proyecto 2 no son idempotentes por diseño: 2.2  
evalúa el corte B una sola vez (según el _rubric_) y 2.4 versiona el  
modelo. La reproducibilidad de P2 se garantiza con `random_state=42` en  
las subfases 1.4 y 2.3.

### Prevención de fuga de información (_data leakage_)

La etiqueta del corte B se lee únicamente en `p2_umbral_test_unico.py`,  
con una validación que aborta si la bitácora ya contiene una evaluación  
previa.

### Seguridad

Los identificadores se seudonimizan con SHA-256. No se persisten números  
de tarjeta en Gold.

### Datos simulados

Los 9 archivos del Proyecto 2 son sintéticos (90 días, aproximadamente  
270 MB). Por lo tanto, los resultados no reflejan una operación real.