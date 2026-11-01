import json
import os
import sys
import time
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

BRONZE_DIR = "./bronze"

KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': os.getenv('KAFKA_GROUP_ID', f'amt_bronze_consumer_group_{uuid.uuid4().hex[:8]}'),
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'enable.partition.eof': False,
    'fetch.max.bytes': 50 * 1024 * 1024,
    'max.partition.fetch.bytes': 10 * 1024 * 1024,
    'queued.min.messages': 100_000,
    'session.timeout.ms': 30_000,
}

MAX_BATCH_SIZE = 5_000
MAX_EMPTY_POLLS_IDLE = 30      # antes del primer mensaje
MAX_EMPTY_POLLS_DRAIN = 10     # despues del primer mensaje
REPORT_EVERY = 10_000          # log de progreso cada N mensajes recibidos


# ---------------------------------------------------------------------------
# Log ASCII-safe
# ---------------------------------------------------------------------------
def _log(msg: str):
    """Print con flush y saneado a ASCII para evitar UnicodeEncodeError en cp1252."""
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        # fallback: reemplaza lo no-ASCII por '?'
        print(msg.encode("ascii", errors="replace").decode("ascii"), flush=True)


# ---------------------------------------------------------------------------
# Escritura a Parquet
# ---------------------------------------------------------------------------
def guardar_batch_bronze(records: list, dataset_name: str, batch_num: int = 0) -> int:
    if not records:
        return 0

    df = pd.DataFrame(records, copy=False)
    records.clear()  # libera la lista original cuanto antes

    ahora = datetime.now(timezone.utc)
    df["ingestion_timestamp"] = ahora.isoformat()
    df["source_file"] = f"{dataset_name}.stream"

    if "timestamp_utc" in df.columns:
        col_fecha = "timestamp_utc"
    else:
        candidatos = [c for c in df.columns
                      if "fecha" in c.lower() or "timestamp" in c.lower() or "date" in c.lower()]
        col_fecha = candidatos[0] if candidatos else None

    if col_fecha:
        fechas = pd.to_datetime(df[col_fecha], errors="coerce").dt.strftime("%Y/%m/%d")
        df["_part_fecha"] = fechas.fillna(ahora.strftime("%Y/%m/%d"))
    else:
        df["_part_fecha"] = ahora.strftime("%Y/%m/%d")

    n = len(df)
    for particion, sub_df in df.groupby("_part_fecha"):
        ruta_salida = os.path.join(BRONZE_DIR, dataset_name, particion)
        os.makedirs(ruta_salida, exist_ok=True)
        datos = sub_df.drop(columns=["_part_fecha"])
        tabla = pa.Table.from_pandas(datos, preserve_index=False)
        nombre_archivo = f"{dataset_name}_batch_{batch_num:05d}.parquet"
        pq.write_table(tabla, os.path.join(ruta_salida, nombre_archivo), compression="SNAPPY")
        del tabla, datos, sub_df

    del df
    return n


def _flush_buffer(consumer, buffer, batch_counter, conteo_total) -> bool:
    """Flushea buffers y commitea si hubo algo. Devuelve True si commiteo."""
    hubo = False
    for d_name in list(buffer.keys()):
        if buffer[d_name]:
            batch_counter[d_name] += 1
            n = guardar_batch_bronze(buffer[d_name], d_name, batch_counter[d_name])
            conteo_total[d_name] += n
            buffer[d_name] = []
            hubo = True
            _log(
                f"  [SAVE] {d_name} batch {batch_counter[d_name]:05d}: "
                f"{n:,} filas (acumulado {conteo_total[d_name]:,})"
            )
    if hubo:
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            _log(f"  [WARN] commit fallo: {e}")
    return hubo


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def consumir_hacia_bronze():
    _log("=" * 60)
    _log(" STREAMING KAFKA -> BRONZE (Subfase 1.1)")
    _log("=" * 60)
    _log(f"[INFO] group.id = {KAFKA_CONF['group.id']}")
    _log(f"[INFO] batch max = {MAX_BATCH_SIZE:,} mensajes por flush")

    consumer = Consumer(KAFKA_CONF)
    topics = ['transmetro_topic', 'aerometro_topic']
    consumer.subscribe(topics)
    _log(f"[INFO] Suscrito a topics: {topics}")
    _log("[WAIT] Haciendo JoinGroup/SyncGroup (poll inicial 10s)...")

    # Primer poll largo: fuerza rebalance y espera datos reales del broker
    _ = consumer.poll(timeout=10.0)
    _log("[OK] Rebalance completado. Esperando mensajes...")

    buffer = {'transmetro_validaciones': [], 'aerometro_boardings': []}
    conteo_total = {'transmetro_validaciones': 0, 'aerometro_boardings': 0}
    batch_counter = {'transmetro_validaciones': 0, 'aerometro_boardings': 0}

    t_inicio = time.time()
    proximo_reporte = REPORT_EVERY

    try:
        empty_polls = 0
        recibidos = 0
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                empty_polls += 1
                limite = MAX_EMPTY_POLLS_IDLE if recibidos == 0 else MAX_EMPTY_POLLS_DRAIN

                # Flush periodico mientras no hay mensajes
                if empty_polls % 5 == 0:
                    _flush_buffer(consumer, buffer, batch_counter, conteo_total)

                # Log de espera cada 10s para que no parezca colgado
                if empty_polls % 10 == 0 and empty_polls < limite:
                    _log(
                        f"[WAIT] Sin mensajes aun ({empty_polls}s, "
                        f"recibidos={recibidos:,}, limite={limite}s)..."
                    )

                if empty_polls >= limite:
                    _log(
                        f"[INFO] Sin mensajes por {limite}s "
                        f"(recibidos totales={recibidos:,}). Cerrando consumidor..."
                    )
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                _log(f"[WARN] Error Kafka: {msg.error()}")
                break

            # Mensaje valido
            empty_polls = 0
            recibidos += 1
            dataset = 'transmetro_validaciones' if msg.topic() == 'transmetro_topic' else 'aerometro_boardings'
            buffer[dataset].append(json.loads(msg.value().decode('utf-8')))

            # Progreso cada REPORT_EVERY mensajes
            if recibidos >= proximo_reporte:
                dt = time.time() - t_inicio
                rate = recibidos / dt if dt > 0 else 0
                _log(
                    f"[INFO] Recibidos {recibidos:,} mensajes "
                    f"({rate:,.0f}/s) | "
                    f"transmetro={conteo_total['transmetro_validaciones']:,} "
                    f"aerometro={conteo_total['aerometro_boardings']:,}"
                )
                proximo_reporte += REPORT_EVERY

            if len(buffer[dataset]) >= MAX_BATCH_SIZE:
                _flush_buffer(consumer, buffer, batch_counter, conteo_total)

    except KeyboardInterrupt:
        _log("[WARN] Interrumpido por el usuario (Ctrl+C).")
    finally:
        _log("[INFO] Flush final de buffers pendientes...")
        _flush_buffer(consumer, buffer, batch_counter, conteo_total)

        try:
            consumer.commit(asynchronous=False)
            _log("[OK] Offsets commiteados (commit final).")
        except Exception as e:
            _log(f"[WARN] No se pudo commitear offsets: {e}")

        try:
            consumer.close()
            _log("[OK] Consumer cerrado.")
        except Exception as e:
            _log(f"[WARN] consumer.close() fallo: {e}")

        # -------------------------------------------------------------
        # Metricas
        # -------------------------------------------------------------
        os.makedirs("results", exist_ok=True)
        filas = [{"topico_dataset": k, "registros_ingeridos": v} for k, v in conteo_total.items()]
        df_stream = pd.DataFrame(filas)
        total_eventos = int(df_stream["registros_ingeridos"].sum()) if not df_stream.empty else 0

        ruta_csv = "results/fase1_1.1_ingesta_streaming_conteos.csv"
        df_stream.to_csv(ruta_csv, index=False, encoding="utf-8")
        _log(f"[OK] Metricas de streaming: {ruta_csv}")

        ruta_md = "results/fase1_1.1_ingesta_streaming_resumen.md"
        with open(ruta_md, "w", encoding="utf-8") as f:
            f.write("# Reporte de Ingesta Streaming (Subfase 1.1)\n\n")
            f.write("- **Origen:** Consumidor Kafka (Topicos Transmetro y Aerometro)\n")
            f.write("- **Destino:** Capa Bronze (Parquet SNAPPY, particionado YYYY/MM/DD)\n")
            f.write(f"- **Total Registros Consumidos:** {total_eventos:,}\n\n")
            f.write("## Detalle de Registros por Topico\n\n")
            f.write(df_stream.to_markdown(index=False))
            f.write("\n")
        _log(f"[OK] Reporte de streaming: {ruta_md}")

        dt_total = time.time() - t_inicio
        _log("=" * 60)
        _log(
            f"[OK] CONSUMER COMPLETADO: {total_eventos:,} eventos en {dt_total:.1f}s"
        )
        _log("=" * 60)

        # Falla con mensaje claro si no se consumio nada
        if total_eventos == 0:
            raise RuntimeError(
                "El consumer no recibio ningun mensaje de Kafka. "
                "Revisa timing producer->broker->consumer y logs del broker."
            )


if __name__ == "__main__":
    consumir_hacia_bronze()