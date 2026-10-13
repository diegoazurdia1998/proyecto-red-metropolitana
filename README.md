# Red Metropolitana de Transporte (AMT)
## Pipeline de Ingeniería de Datos y Modelado Dimensional

Este repositorio contiene la solución técnica desarrollada para la integración, control de calidad, historización y modelado analítico de los cuatro modos de transporte público del área metropolitana de Guatemala: **Transmetro**, **Transurbano**, **MetroRiel** y **Aerómetro**.

El proyecto implementa una **Arquitectura Medallion** (Bronze, Staging, Silver y Gold), orquestada de forma idempotente con **Prefect**, transformaciones SQL estructuradas mediante **dbt Core**, almacenamiento relacional en **PostgreSQL**, ingesta de eventos por **Apache Kafka** y un Data Lake columnar particionado en formato **Apache Parquet**.

---

## Estructura de Entregables (`results/`)

Todos los artefactos generados siguen el estándar de nomenclatura `[fase]_[subfase]_[contenido].extension`:

  

| **Archivo**                                 | **Formato** | **Contenido**                                                          |
| ------------------------------------------- | ----------- | ---------------------------------------------------------------------- |
| `fase1_1.1_ingesta_batch_cdc_conteos_*.csv` | CSV         | Registro de filas procesadas por archivo fuente hacia Bronze.          |
| `fase1_1.1_ingesta_batch_cdc_resumen_*.md`  | Markdown    | Bitácora técnica y justificación de arquitectura de Data Lake.         |
| `fase1_1.1_ingesta_streaming_conteos_*.csv` | CSV         | Conteos por tópico de streaming consumidos hacia Bronze.               |
| `fase1_1.2_padron_vigente_transmetro_*.csv` | CSV         | Padrón procesado con balance final de altas y bajas por CDC.           |
| `fase1_1.2_catalogos_usuarios_*.csv`        | CSV         | Extracción de llaves únicas por operador para catálogos mínimos.       |
| `fase1_1.2_resumen_cdc_conteos_*.md`        | Markdown    | Resumen documental del balance CDC y usuarios únicos intermodales.     |
| `fase1_1.3_metricas_cuarentena_*.csv`       | CSV         | Distribución de registros rechazados por fuente y regla de validación. |
| `fase1_1.3_reporte_calidad_cuarentena_*.md` | Markdown    | Reporte formal de auditoría y supuestos de identidad de usuario.       |
| `fase1_1.5_conteos_corridas_*.csv`          | CSV         | Matriz de auditoría comparativa de conteos (Corrida 1 vs. Corrida 2).  |
| `fase1_1.5_evidencia_idempotencia_*.md`     | Markdown    | Certificación técnica del principio de idempotencia del pipeline.      |
=======
Todos los artefactos y reportes generados siguen el estándar de nomenclatura `[fase]_[subfase]_[contenido].extension`:

| Archivo | Formato | Descripción |
| :--- | :--- | :--- |
| `fase1_1.1_ingesta_batch_cdc_conteos_*.csv` | CSV | Registro de filas procesadas por archivo fuente hacia la capa Bronze. |
| `fase1_1.1_ingesta_batch_cdc_resumen_*.md` | Markdown | Bitácora técnica y justificación de la arquitectura del Data Lake. |
| `fase1_1.1_ingesta_streaming_conteos_*.csv` | CSV | Conteos de mensajes consumidos por tópico desde Kafka hacia Bronze. |
| `fase1_1.2_padron_vigente_transmetro_*.csv` | CSV | Padrón procesado con el balance final de altas, cambios y bajas (CDC). |
| `fase1_1.2_catalogos_usuarios_*.csv` | CSV | Catálogos mínimos con identificadores únicos por operador. |
| `fase1_1.2_resumen_cdc_conteos_*.md` | Markdown | Documentación del balance de CDC y conteo de usuarios únicos. |
| `fase1_1.3_metricas_cuarentena_*.csv` | CSV | Desglose de registros rechazados por sistema y regla de calidad. |
| `fase1_1.3_reporte_calidad_cuarentena_*.md` | Markdown | Informe de auditoría de calidad y supuestos de identidad de usuario. |
| `fase1_1.5_conteos_corridas_*.csv` | CSV | Matriz comparativa de auditoría (Corrida 1 vs. Corrida 2). |
| `fase1_1.5_evidencia_idempotencia_*.md` | Markdown | Demostración técnica del principio de idempotencia del pipeline. |
| `fase1_1.5_log_ejecucion_*.log` | Log | Registro detallado de terminal y eventos emitidos por Prefect. |

---

## 1. Arquitectura del Flujo de Datos

```
[Fuentes de Datos]
  │  ├── Transmetro  (Batch CSV + Kafka Streaming + CDC log)
  │  ├── Transurbano (Batch CSV)
  │  ├── MetroRiel   (Batch JSONL con trayectos anidados)
  │  └── Aerómetro   (Kafka Streaming, timestamp UTC, hash)
  ▼
[Capa Bronze (Data Lake)]
  │  └── Archivos Parquet (compresión SNAPPY, partición física YYYY/MM/DD)
  ▼
[Capa Staging (PostgreSQL - Efímero)]
  │  ├── Vaciado y reconstrucción en cada corrida (DROP/TRUNCATE)
  │  ├── CDC del padrón con soft-deletes (es_activa = FALSE)
  │  └── Catálogos de usuarios y semillas de estaciones
  ▼
[Capa Silver (Calidad, SCD2 y Limpieza)]
  │  ├── silver.cuarentena (Aislamiento de fallas con payload JSONB)
  │  ├── Reglas: Duplicados torniquete (<=120s), nulos, futuro, sin salida
  │  └── SCD Tipo 2: snap_padron_transmetro (dbt snapshot)
  ▼
[Capa Gold (Data Warehouse Dimensional - Kimball)]
     ├── Dimensiones conformadas: dim_tiempo, dim_usuario (SHA-256), dim_estacion_parada
     └── Tablas de hechos: fact_abordajes (unificada) y fact_viajes_metroriel (acumulada)

```

---

## 2. Requisitos del Sistema

* **Docker y Docker Compose** (para PostgreSQL y Apache Kafka KRaft).
* **Python 3.10 o superior** (con entorno virtual activado).
* **dbt Core** con el conector `dbt-postgres` (`v1.8+`).
* **Prefect** (`v3.0+`).

---

## 3. Configuración del Entorno

### 3.1. Clonar el repositorio y preparar el entorno virtual

```bash
git clone [https://github.com/diegoazurdia1998/proyecto-red-metropolitana](https://github.com/diegoazurdia1998/proyecto-red-metropolitana)
cd proyecto-red-metropolitana

# Crear y activar el entorno virtual (ejemplo en Windows PowerShell)
python -m venv venv
.\venv\Scripts\Activate.ps1

# Instalar librerías requeridas
pip install -r requirements.txt
````

### 3.2. Configuración de credenciales (`.env`)

Crea un archivo `.env` en la raíz del proyecto a partir de la siguiente plantilla:

Fragmento de código

```
DB_HOST=localhost
DB_PORT=5432
DB_USER=amt_admin
DB_PASSWORD=amt_secure_pass
DB_NAME=amt_dw
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

> **Nota de seguridad:** El archivo `.env` está excluido del control de versiones en `.gitignore` para evitar la exposición de credenciales.

### 3.3. Despliegue de infraestructura local

Inicia los servicios en segundo plano:

Bash

```
docker-compose up -d
```

Verifica que los contenedores de PostgreSQL y Kafka se encuentren en ejecución:

Bash

```
docker ps
```

## 4. Ejecución del Pipeline y Verificación de Idempotencia

Para ejecutar el ciclo de procesamiento completo (desde la ingesta hasta la materialización en Gold), validar la idempotencia mediante dos corridas consecutivas y compilar los reportes de auditoría en `results/`, ejecuta:

Bash

```
python .\scripts\orchestration\test_idempotencia.py
```

El script verifica automáticamente que la diferencia en el conteo de filas entre ambas corridas sea exactamente **cero** en todas las tablas del almacén.

## 5. Consultas de Verificación en Base de Datos

Para auditar directamente los datos en PostgreSQL, ingresa a la sesión interactiva del contenedor:

Bash

```
docker exec -it amt_postgres psql -U amt_admin -d amt_dw
```

### Consultas de validación rápida:

SQL

```
-- 1. Distribución de registros rechazados en cuarentena
SELECT fuente, regla_violada, COUNT(*) AS total_rechazos 
FROM silver.cuarentena 
GROUP BY 1, 2;

-- 2. Estado del padrón de usuarios activo (SCD Tipo 2 vigente)
SELECT perfil, es_activa, COUNT(*) AS total_usuarios 
FROM silver.snap_padron_transmetro 
WHERE dbt_valid_to IS NULL 
GROUP BY 1, 2;

-- 3. Resumen de abordajes e ingresos recaudados en la capa Gold
SELECT sistema_modo, COUNT(*) AS total_abordajes, SUM(monto_gtq) AS recaudo_total_gtq 
FROM gold.fact_abordajes 
GROUP BY 1;
```
