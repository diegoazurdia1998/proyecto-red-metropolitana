import subprocess
import sys
import os
import json
import hashlib
import pandas as pd


def correr_pipeline(label: str, limpiar: bool) -> dict:
    """Ejecuta el flow en subproceso, muestra la salida en vivo y devuelve los conteos."""
    codigo = (
        "import json; "
        "from scripts.orchestration.pipeline_flow import red_metropolitana_pipeline; "
        f"c = red_metropolitana_pipeline(corrida_label={label!r}, limpiar={limpiar}); "
        "print('__CONTEOS__' + json.dumps(c), flush=True)"
    )
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    process = subprocess.Popen(
        [sys.executable, "-c", codigo],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=env,
    )

    lineas = []
    conteos = None
    try:
        for line in process.stdout:
            line = line.rstrip("\n")
            lineas.append(line)
            print(line, flush=True)  # flujo en vivo
            if line.startswith("__CONTEOS__"):
                conteos = json.loads(line[len("__CONTEOS__"):])
    finally:
        process.stdout.close()
        process.wait()

    if process.returncode != 0:
        tail = "\n".join(lineas[-50:])
        raise RuntimeError(
            f"Fallo en {label} (codigo {process.returncode}):\n"
            f"--- ultimas 50 lineas ---\n{tail}"
        )
    if conteos is None:
        tail = "\n".join(lineas[-50:])
        raise RuntimeError(
            f"No se encontraron conteos en la salida de {label}.\n"
            f"--- ultimas 50 lineas ---\n{tail}"
        )
    return conteos


def _hash_bronze() -> str:
    """Hash determinista del contenido de bronze/ (para comparar entre corridas)."""
    h = hashlib.md5()
    for root, dirs, files in os.walk("bronze"):
        dirs.sort()
        for f in sorted(files):
            ruta = os.path.join(root, f)
            rel = os.path.relpath(ruta, "bronze").replace("\\", "/")
            h.update(rel.encode())
            with open(ruta, "rb") as fh:
                while True:
                    chunk = fh.read(1 << 20)
                    if not chunk:
                        break
                    h.update(chunk)
    return h.hexdigest()


def ejecutar_prueba_idempotencia():
    print("=" * 70)
    print(" TEST DE IDEMPOTENCIA - Subfase 1.5")
    print(" Semantica: reejecutar desde cero produce el mismo resultado")
    print("=" * 70)

    print("\n--- CORRIDA 1 (limpieza total) ---")
    conteos_1 = correr_pipeline("CORRIDA 1", limpiar=True)
    hash_1 = _hash_bronze()
    print(f"[HASH] Bronze corrida 1: {hash_1}")

    print("\n--- CORRIDA 2 (limpieza total) ---")
    conteos_2 = correr_pipeline("CORRIDA 2", limpiar=True)
    hash_2 = _hash_bronze()
    print(f"[HASH] Bronze corrida 2: {hash_2}")

    print("\n" + "=" * 70)
    print(" RESULTADOS")
    print("=" * 70)

    es_identico = True
    filas = []
    for tabla in sorted(conteos_1.keys()):
        c1 = conteos_1[tabla]
        c2 = conteos_2.get(tabla, -1)
        if c1 != c2:
            es_identico = False
        filas.append({
            "tabla": tabla,
            "conteo_corrida_1": c1,
            "conteo_corrida_2": c2,
            "diferencia": c2 - c1,
        })

    df_diff = pd.DataFrame(filas)
    print(df_diff.to_string(index=False))


    veredicto = "IDEMPOTENTE" if es_identico else "FALLO"
    print(f"\nVEREDICTO: {veredicto}")

    os.makedirs("results", exist_ok=True)
    df_diff.to_csv("results/fase1_1.5_conteos_corridas.csv", index=False)

    with open("results/fase1_1.5_evidencia_idempotencia.md", "w", encoding="utf-8") as f:
        f.write("# Evidencia de Idempotencia (Subfase 1.5)\n\n")
        f.write("## Semantica\n\n")
        f.write("Reejecutar el pipeline completo desde cero (limpieza total) produce el mismo resultado.\n\n")
        f.write("## Conteos por tabla\n\n")
        f.write(df_diff.to_markdown(index=False))
        f.write(f"## Veredicto\n\n**{veredicto}**\n")

    print(f"\nEvidencia guardada en results/fase1_1.5_*")
    return veredicto


if __name__ == "__main__":
    ejecutar_prueba_idempotencia()