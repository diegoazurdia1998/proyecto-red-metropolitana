# Red Metropolitana de Transporte (AMT) - Fase 1: Ingeniería de Datos y Almacenamiento

Pipeline de ingeniería de datos para la unificación, control de calidad, historización y modelado dimensional analítico de los cuatro modos de transporte público de la Ciudad de Guatemala: **Transmetro**, **Transurbano**, **MetroRiel** y **Aerómetro**.

  

El proyecto implementa una **Arquitectura Medallion** (Bronze, Staging, Silver y Gold) orquestada con **Prefect**, transformaciones SQL declarativas con **dbt Core**, almacenamiento relacional en **PostgreSQL**, ingesta desacoplada vía **Apache Kafka** y un Data Lake columnar particionado en formato **Parquet**.

  
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

## 2. Requisitos Previos

- **Docker y Docker Compose** (para PostgreSQL y Kafka KRaft).
    
      
    
- **Python 3.10+** (con entorno virtual configurado).
    
      
    
- **dbt Core** con adaptador `dbt-postgres` (`v1.8+` o compatible).
    
      
    
- **Prefect** (`v3.0+`).
    
      
    

## 3. Configuración Inicial del Entorno

### Clonar el repositorio y preparar el entorno virtual

```
git clone https://github.com/diegoazurdia1998/proyecto-red-metropolitana
cd proyecto-red-metropolitana

# Crear y activar entorno virtual en Windows PowerShell
python -m venv venv
.\venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt
```

### Configurar variables de entorno (`.env`)

Crea un archivo `.env` en la raíz del proyecto:

```
DB_HOST=localhost
DB_PORT=5432
DB_USER=amt_admin
DB_PASSWORD=amt_secure_pass
DB_NAME=amt_dw
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### Levantar los contenedores de infraestructura

```
docker-compose up -d
```

Verifica que PostgreSQL y Kafka estén saludables:

```
docker ps
```

## 4. Ejecución del Pipeline

### Ejecución Automatizada y Validación de Idempotencia 

Para ejecutar el ciclo de vida completo de extremo a extremo, validar la idempotencia (Corrida 1 vs. Corrida 2) y generar todas las evidencias tabulares y documentales en `results/`:

```
python .\scripts\orchestration\test_idempotencia.py
```

Para generar:

```
python .\scripts\orchestration\test_idempotencia.py 
```

## 6. Verificación de la Base de Datos

Puedes consultar directamente el Data Warehouse conectándote al contenedor:

```
docker exec -it amt_postgres psql -U amt_admin -d amt_dw
```

### Consultas de validación rápida:

```
-- Verificar balance de cuarentena
SELECT fuente, regla_violada, COUNT(*) FROM silver.cuarentena GROUP BY 1, 2;

-- Verificar vigencia de usuarios en SCD Tipo 2
SELECT perfil, es_activa, COUNT(*) FROM silver.snap_padron_transmetro WHERE dbt_valid_to IS NULL GROUP BY 1, 2;

-- Validar totales en la tabla de hechos unificada
SELECT modo, COUNT(*), SUM(monto_gtq) FROM gold.fact_abordajes GROUP BY 1;
```
