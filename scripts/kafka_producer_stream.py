import csv
import json
import os
import sys
import time
from confluent_kafka import Producer

KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 50,
    'batch.num.messages': 10_000,
    'batch.size': 1_048_576,
    'compression.type': 'snappy',
    'queue.buffering.max.messages': 200_000,
    'queue.buffering.max.kbytes': 512_000,
    'acks': '1',
}

DATOS_RED_DIR = "./scripts/datos_red"
FLUSH_EVERY = 50_000
IS_TTY = sys.stdout.isatty()


def _log(msg: str):
    print(msg, flush=True)


def _contar_lineas(ruta: str) -> int:
    with open(ruta, "rb") as f:
        return sum(1 for _ in f)


def delivery_report(err, msg):
    if err is not None:
        _log(f"  !  Fallo en entrega: {err}")


def _get_key(row: dict, topic: str) -> str:
    if "aerometro" in topic:
        return str(row.get("user_hash") or "key")
    return str(
        row.get("tarjeta")
        or row.get("num_tarjeta")
        or row.get("id_tarjeta")
        or "key"
    )


def _stream_archivo(producer: Producer, archivo_csv: str, topic: str):
    ruta = os.path.join(DATOS_RED_DIR, archivo_csv)
    total_lineas = _contar_lineas(ruta) - 1
    _log(f"[{topic}] {archivo_csv}: ~{total_lineas:,} filas")
    t0 = time.time()
    count = 0
    proximo_reporte = max(1, total_lineas // 20)   # ~20 líneas de progreso

    with open(ruta, mode="r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.produce(
                topic,
                key=_get_key(row, topic),
                value=json.dumps(row, separators=(",", ":")).encode("utf-8"),
                callback=delivery_report,
            )
            count += 1

            if count % 1000 == 0:
                producer.poll(0)
            if count % FLUSH_EVERY == 0:
                producer.flush()

            if count >= proximo_reporte:
                pct = 100.0 * count / total_lineas if total_lineas else 0
                _log(f"  [{topic}] {count:,}/{total_lineas:,} ({pct:5.1f}%)")
                proximo_reporte += max(1, total_lineas // 20)

    producer.flush()
    dt = time.time() - t0
    _log(f"[{topic}] {count:,} eventos enviados en {dt:.1f}s")
    return count


def simular_streaming():
    producer = Producer(KAFKA_CONF)
    streams = [
        ("transmetro_validaciones.csv", "transmetro_topic"),
        ("aerometro_boardings.csv", "aerometro_topic"),
    ]
    _log("=" * 60)
    _log(" STREAMING -> KAFKA (Subfase 1.1)")
    _log("=" * 60)
    t_inicio = time.time()
    total_global = 0
    for archivo_csv, topic in streams:
        total_global += _stream_archivo(producer, archivo_csv, topic)
    _log("=" * 60)
    _log(f" OK STREAMING COMPLETADO: {total_global:,} eventos en {time.time()-t_inicio:.1f}s")
    _log("=" * 60)


if __name__ == "__main__":
    simular_streaming()