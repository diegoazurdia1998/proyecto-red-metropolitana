import os
import sys
import subprocess
import psycopg2
from dotenv import load_dotenv
from prefect import task, flow
from prefect.logging import get_run_logger

load_dotenv()

def run_script(script_path: str, label: str = ""):
    """Ejecuta un script Python mostrando su salida en vivo en los logs de Prefect."""
    logger = get_run_logger()
    logger.info(f"▶️  Iniciando {label or script_path} ...")

    process = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    for line in process.stdout:
        logger.info(f"[{label or script_path}] {line.rstrip()}")
    process.wait()

    if process.returncode != 0:
        raise RuntimeError(f"Fallo en {script_path} (código {process.returncode})")
    logger.info(f"✅ {label or script_path} completado.")

def run_command(cmd: list[str], label: str = ""):
    logger = get_run_logger()
    logger.info(f"▶️  {label}: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    for line in process.stdout:
        logger.info(f"[{label}] {line.rstrip()}")
    process.wait()
    if process.returncode != 0:
        raise RuntimeError(f"Fallo en {label} (código {process.returncode})")
    logger.info(f"✅ {label} completado.")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD", "amt_secure_pass"),
        dbname=os.getenv("DB_NAME", "amt_dw")
    )

@task(name="Ingesta Capa Bronze (Batch/CDC)")
def task_ingesta_bronze():
    """Ejecuta la ingesta batch y CDC hacia Parquet en Bronze."""
    run_script("scripts/batch_cdc_to_bronze.py", label="Bronze")
    return "Bronze Batch/CDC completado."

@task(name="Vaciado y Carga de Staging Efímero")
def task_cargar_staging():
    """Garantiza la regla de arquitectura: Staging se vacía en cada corrida."""
    run_script("scripts/staging_cdc_transmetro.py", label="Staging-CDC")
    run_script("scripts/staging_catalogos_minimos.py", label="Staging-Catálogos")
    run_script("scripts/cargar_staging_operaciones.py", label="Staging-Operaciones")
    return "Staging reiniciado y poblado exitosamente."

@task(name="Transformaciones dbt (Silver, Snapshot SCD2, Gold)")
def task_ejecutar_dbt():
    """Ejecuta snapshots y modelos dimensionales asegurando idempotencia."""
    run_command(
        ["dbt", "snapshot", "--project-dir", "dbt_transforms", "--profiles-dir", "dbt_transforms"],
        label="dbt-snapshot",
    )
    run_command(
        ["dbt", "run", "--project-dir", "dbt_transforms", "--profiles-dir", "dbt_transforms"],
        label="dbt-run",
    )
    return "Modelos dbt compilados y ejecutados exitosamente."

@task(name="Enrutamiento a Cuarentena y Calidad")
def task_cuarentena():
    """Enruta registros inválidos hacia silver.cuarentena y calcula métricas."""
    run_script("scripts/cargar_cuarentena_y_metricas.py", label="Cuarentena y Calidad")
    return "Cuarentena procesada correctamente."

@task(name="Medición de Idempotencia y Conteos")
def task_medir_conteos(corrida_label: str):
    """Consulta los conteos exactos por tabla para auditoría de idempotencia."""
    conn = get_db_connection()
    cur = conn.cursor()

    tablas = [
        ("silver", "cuarentena"),
        ("silver", "snap_padron_transmetro"),
        ("gold", "dim_usuario"),
        ("gold", "dim_tiempo"),
        ("gold", "fact_abordajes"),
        ("gold", "fact_viajes_metroriel")
    ]

    conteos = {}
    for schema, tabla in tablas:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{tabla};")
        conteos[f"{schema}.{tabla}"] = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"\n==========================================")
    print(f" RESULTADOS DE CONTEOS: {corrida_label}")
    print(f"==========================================")
    for tabla, total in conteos.items():
        print(f" {tabla:<32} : {total:>8} filas")
    print(f"==========================================\n")

    return conteos

@flow(name="AMT - Pipeline End-to-End Fase 1")
def red_metropolitana_pipeline(corrida_label: str = "Corrida 1"):
    task_ingesta_bronze()
    task_cargar_staging()
    task_ejecutar_dbt()
    task_cuarentena()
    return task_medir_conteos(corrida_label)

if __name__ == "__main__":
    red_metropolitana_pipeline()