import os
import sys
import subprocess
import shutil
import time

import psycopg2
from contextlib import closing
from dotenv import load_dotenv
from prefect import task, flow
from prefect.logging import get_run_logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
from p2_medir_tiempos import medir_etapa

load_dotenv()


# ---------------------------------------------------------------------------
# Helpers de ejecución de subprocesos (con cierre de pipes para no retener RAM)
# ---------------------------------------------------------------------------
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
    try:
        for line in process.stdout:
            logger.info(f"[{label or script_path}] {line.rstrip()}")
    finally:
        process.stdout.close()
        process.wait()

    if process.returncode != 0:
        raise RuntimeError(f"Fallo en {script_path} (código {process.returncode})")
    logger.info(f"✅ {label or script_path} completado.")


def run_command(cmd: list[str], label: str = ""):
    logger = get_run_logger()
    logger.info(f"▶️  {label}: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    try:
        for line in process.stdout:
            logger.info(f"[{label}] {line.rstrip()}")
    finally:
        process.stdout.close()
        process.wait()

    if process.returncode != 0:
        raise RuntimeError(f"Fallo en {label} (código {process.returncode})")
    logger.info(f"✅ {label} completado.")


# ---------------------------------------------------------------------------
# Conexión Postgres
# ---------------------------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


# ---------------------------------------------------------------------------
# Limpieza Kafka
# ---------------------------------------------------------------------------
def _p2_limpiar_consumer_groups_kafka(logger):
    """Borra consumer groups viejos 'amt_bronze_consumer_group_*' de corridas anteriores."""
    listar_cmd = [
        "docker", "exec", "amt_kafka",
        "kafka-consumer-groups",
        "--bootstrap-server", "localhost:9092",
        "--list",
    ]
    try:
        proc_list = subprocess.run(listar_cmd, capture_output=True, text=True, timeout=15)
        if proc_list.returncode != 0:
            logger.warning(f"⚠️  No se pudo listar consumer groups: {proc_list.stderr[:200]}")
            return

        grupos_viejos = [
            g.strip()
            for g in proc_list.stdout.splitlines()
            if g.strip() == "amt_bronze_consumer_group" or g.strip().startswith("amt_bronze_consumer_group_")
        ]
        if not grupos_viejos:
            logger.info("ℹ️  No hay consumer groups viejos que limpiar.")
            return

        borrar_cmd = [
            "docker", "exec", "amt_kafka",
            "kafka-consumer-groups",
            "--bootstrap-server", "localhost:9092",
            "--delete",
        ]
        for g in grupos_viejos:
            borrar_cmd.extend(["--group", g])

        proc_del = subprocess.run(borrar_cmd, capture_output=True, text=True, timeout=30)
        if proc_del.returncode == 0:
            logger.info(f"✅ Limpiados {len(grupos_viejos)} consumer groups viejos.")
        else:
            logger.warning(f"⚠️  Falló borrado de groups viejos: {proc_del.stderr[:200]}")
    except Exception as e:
        logger.warning(f"⚠️  No se pudo limpiar consumer groups (no bloqueante): {e}")

def _esperar_topic_vacio(topic: str, timeout_s: int = 30) -> bool:
    """Espera a que el offset final del topic sea 0 (delete completado)."""
    bootstrap = "localhost:9092"
    inicio = time.time()
    while time.time() - inicio < timeout_s:
        proc = subprocess.run(
            ["docker", "exec", "amt_kafka", "kafka-run-class",
             "kafka.tools.GetOffsetShell",
             "--broker-list", bootstrap, "--topic", topic],
            capture_output=True, text=True, timeout=15,
        )
        if proc.returncode == 0:
            offsets = []
            for linea in proc.stdout.strip().splitlines():
                if linea.strip():
                    try:
                        offsets.append(int(linea.split(":")[-1]))
                    except ValueError:
                        pass
            if offsets and max(offsets) == 0:
                return True
        time.sleep(1)
    return False


def _p2_limpiar_topics_kafka(logger):
    """Borra y recrea los topics de ingesta para garantizar idempotencia real.

    Kafka no deduplica: si el producer reenvia los mismos eventos en cada corrida,
    el topic acumula historico. Como el consumer usa group.id nuevo + earliest,
    leeria TODO el historico. Borrar topics al inicio de cada corrida lo evita.
    """
    topics = ["transmetro_topic", "aerometro_topic"]
    bootstrap = "localhost:9092"

    for topic in topics:
        # Borrar (asincrono)
        subprocess.run(
            ["docker", "exec", "amt_kafka", "kafka-topics",
             "--bootstrap-server", bootstrap,
             "--delete", "--topic", topic],
            capture_output=True, text=True, timeout=30,
        )
        time.sleep(2)

        # Recrear con retry
        creado = False
        for intento in range(10):
            proc = subprocess.run(
                ["docker", "exec", "amt_kafka", "kafka-topics",
                 "--bootstrap-server", bootstrap,
                 "--create", "--topic", topic,
                 "--partitions", "1", "--replication-factor", "1"],
                capture_output=True, text=True, timeout=30,
            )
            if proc.returncode == 0:
                creado = True
                break
            if "already exists" in (proc.stderr or "").lower():
                time.sleep(1)
                continue
            logger.warning(f"[WARN] No se pudo recrear {topic}: {(proc.stderr or '')[:200]}")
            break

        if not creado:
            logger.warning(f"[WARN] Timeout recreando {topic}")
            continue

        # Verificar que quedo vacio antes de seguir
        if _esperar_topic_vacio(topic, timeout_s=30):
            logger.info(f"[OK] Topic recreado y vacio: {topic}")
        else:
            raise RuntimeError(
                f"[ERROR] El topic {topic} no quedo vacio tras 30s. "
                f"Abortando para no contaminar el test de idempotencia."
            )

# ---------------------------------------------------------------------------
# Limpieza de entorno
# ---------------------------------------------------------------------------
def limpiar_bronze(limpiar_results: bool = True):
    """Limpieza de entorno: Bronze + results + consumer groups Kafka.

    - limpiar_results=True  → borra results/ (corrida "desde cero").
    - limpiar_results=False → conserva results/ (corrida idempotente sin limpiar).
    """
    logger = get_run_logger()
    BRONZE_DIR = "./bronze"
    RESULTS_DIR = "./results"

    if os.path.exists(BRONZE_DIR):
        shutil.rmtree(BRONZE_DIR)
    os.makedirs(BRONZE_DIR, exist_ok=True)

    if limpiar_results and os.path.exists(RESULTS_DIR):
        shutil.rmtree(RESULTS_DIR)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    logger.info("ℹ️  Kafka: consumer usará group.id único por corrida (idempotencia garantizada).")
    _p2_limpiar_consumer_groups_kafka(logger)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
@task(name="Ingesta Capa Bronze (Batch/CDC)")
def task_ingesta_bronze(limpiar: bool = True):
    logger = get_run_logger()
    with medir_etapa("ingesta_bronze"):
        if limpiar:
            limpiar_bronze(limpiar_results=True)
        else:
            os.makedirs("./bronze", exist_ok=True)
            os.makedirs("./results", exist_ok=True)

        # Topics SIEMPRE se limpian: el producer reenvia 1.1M y el consumer
        # debe leer exactamente esos 1.1M, no el historico acumulado.
        _p2_limpiar_topics_kafka(logger)

        run_script("scripts/batch_cdc_to_bronze.py", label="Bronze Batch CDC")
        run_script("scripts/kafka_producer_stream.py", label="Bronze Stream Producer")
        logger.info("[WAIT] Esperando 10s para que el broker asiente los mensajes...")
        time.sleep(10)
        run_script("scripts/kafka_consumer_to_bronze.py", label="Bronze Stream Consumer")
    return "Bronze Batch/CDC completado."


@task(name="Vaciado y Carga de Staging Efímero")
def task_cargar_staging():
    with medir_etapa("cargar_staging"):
        """Garantiza la regla de arquitectura: Staging se vacía en cada corrida."""
        run_script("scripts/staging_cdc_transmetro.py", label="Staging-CDC")
        run_script("scripts/staging_catalogos_minimos.py", label="Staging-Catálogos")
        run_script("scripts/cargar_staging_operaciones.py", label="Staging-Operaciones")
    return "Staging reiniciado y poblado exitosamente."


@task(name="Transformaciones dbt (Silver, Snapshot SCD2, Gold)")
def task_ejecutar_dbt():
    with medir_etapa("ejecutar_dbt"):
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
    with medir_etapa("cuarentena"):
        """Enruta registros inválidos hacia silver.cuarentena y calcula métricas."""
        run_script("scripts/crear_cuarentena.py", label="Cuarentena y Calidad")
        run_script("scripts/cargar_cuarentena_y_metricas.py", label="Cuarentena y Calidad")
    return "Cuarentena procesada correctamente."


@task(name="Medición de Idempotencia y Conteos")
def task_medir_conteos(corrida_label: str):
    with medir_etapa("medir_conteos"):
        """Consulta los conteos exactos por tabla para auditoría de idempotencia."""
        tablas = [
            ("silver", "cuarentena"),
            ("silver", "snap_padron_transmetro"),
            ("gold", "dim_usuario"),
            ("gold", "dim_tiempo"),
            ("gold", "dim_estacion_parada"),
            ("gold", "fact_abordajes"),
        ]

        conteos = {}
        with closing(get_db_connection()) as conn:
            with conn.cursor() as cur:
                for schema, tabla in tablas:
                    cur.execute(f"SELECT COUNT(*) FROM {schema}.{tabla};")
                    conteos[f"{schema}.{tabla}"] = cur.fetchone()[0]

        print("\n==========================================")
        print(f" RESULTADOS DE CONTEOS: {corrida_label}")
        print("==========================================")
        for tabla, total in conteos.items():
            print(f" {tabla:<32} : {total:>8} filas")
        print("==========================================\n")

    return conteos


@task(name="P2 - Auditoría 1.1 (9 archivos + tiempos)")
def p2_auditoria_ingesta():
    with medir_etapa("p2_auditoria_ingesta"):
        run_script("scripts/p2_auditoria_ingesta_9_archivos.py", label="P2 Auditoría 1.1")
    return "Auditoría 1.1 completada."


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------
@flow(name="AMT - Pipeline End-to-End P2")
def red_metropolitana_pipeline(corrida_label: str = "Corrida P2", limpiar: bool = True):
    task_ingesta_bronze(limpiar=limpiar)
    task_cargar_staging()
    task_ejecutar_dbt()
    task_cuarentena()
    conteos = task_medir_conteos(corrida_label)
    p2_auditoria_ingesta()
    return conteos


if __name__ == "__main__":
    red_metropolitana_pipeline()