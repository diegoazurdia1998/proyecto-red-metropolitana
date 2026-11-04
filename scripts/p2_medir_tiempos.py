"""
Helper de medición de tiempos por etapa para el rubric 1.1 del P2.

Uso:
    from p2_medir_tiempos import medir_etapa

    with medir_etapa("ingesta_bronze"):
        ...trabajo...

Escribe/append a results/p2_tiempos_etapa.csv con columnas:
    etapa, start, end, duration_s
"""
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
import pandas as pd

RESULTS_DIR = "./results"
TIEMPOS_CSV = os.path.join(RESULTS_DIR, "p2_tiempos_etapa.csv")


def _asegurar_results():
    os.makedirs(RESULTS_DIR, exist_ok=True)


@contextmanager
def medir_etapa(nombre: str):
    """Context manager que mide la duración de un bloque y la persiste."""
    _asegurar_results()
    start = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    error = None
    try:
        yield
    except Exception as e:
        error = e
        raise
    finally:
        end = datetime.now(timezone.utc)
        duration_s = round(time.perf_counter() - t0, 4)
        fila = {
            "etapa": nombre,
            "start": start.isoformat(timespec="seconds"),
            "end": end.isoformat(timespec="seconds"),
            "duration_s": duration_s,
            "estado": "ERROR" if error else "OK",
        }
        header = not os.path.exists(TIEMPOS_CSV)
        pd.DataFrame([fila]).to_csv(
            TIEMPOS_CSV, mode="a", header=header, index=False, encoding="utf-8"
        )


def leer_tiempos() -> pd.DataFrame:
    """Devuelve el DataFrame de tiempos (vacío si no existe el archivo)."""
    if not os.path.exists(TIEMPOS_CSV):
        return pd.DataFrame(columns=["etapa", "start", "end", "duration_s", "estado"])
    return pd.read_csv(TIEMPOS_CSV)