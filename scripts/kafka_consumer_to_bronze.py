import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

BRONZE_DIR = "./bronze"
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'amt_bronze_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}


def guardar_batch_bronze(records: list, dataset_name: str):
    if not records:
        return 0

    df = pd.DataFrame(records)
    ahora = datetime.utcnow()
    df["ingestion_timestamp"] = ahora.isoformat()
    df["source_file"] = f"{dataset_name}.stream"

    # Detección explícita de columna temporal para partición física YYYY/MM/DD
    if "timestamp_utc" in df.columns:
        col_fecha = "timestamp_utc"
    else:
        candidatos = [c for c in df.columns if "fecha" in c.lower() or "timestamp" in c.lower() or "date" in c.lower()]
        col_fecha = candidatos[0] if candidatos else None

    if col_fecha:
        fechas = pd.to_datetime(df[col_fecha], errors="coerce").dt.strftime("%Y/%m/%d")
        df["_part_fecha"] = fechas.fillna(ahora.strftime("%Y/%m/%d"))
    else:
        df["_part_fecha"] = ahora.strftime("%Y/%m/%d")

    for particion, sub_df in df.groupby("_part_fecha"):
        ruta_salida = os.path.join(BRONZE_DIR, dataset_name, particion)
        os.makedirs(ruta_salida, exist_ok=True)

        datos = sub_df.drop(columns=["_part_fecha"])
        tabla = pa.Table.from_pandas(datos, preserve_index=False)
        nombre_archivo = f"{dataset_name}_stream_{ahora.strftime('%H%M%S_%f')}.parquet"
        pq.write_table(tabla, os.path.join(ruta_salida, nombre_archivo), compression="SNAPPY")

    return len(records)


def consumir_hacia_bronze():
    consumer = Consumer(KAFKA_CONF)
    topics = ['transmetro_topic', 'aerometro_topic']
    consumer.subscribe(topics)
    print(f"Consumidor escuchando topics {topics}...")

    buffer = {'transmetro_validaciones': [], 'aerometro_boardings': []}
    conteo_total = {'transmetro_validaciones': 0, 'aerometro_boardings': 0}
    max_batch_size = 20000

    # --- control de inactividad ---
    MAX_EMPTY_POLLS = 5          # ~10s

    try:
        empty_polls = 0
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                empty_polls += 1

                # Flush por inactividad
                if empty_polls % 4 == 0:
                    for d_name in buffer:
                        if buffer[d_name]:
                            conteo_total[d_name] += guardar_batch_bronze(buffer[d_name], d_name)
                            buffer[d_name] = []


                if empty_polls >= MAX_EMPTY_POLLS:
                    print(f"Sin mensajes por {MAX_EMPTY_POLLS * 2}s. Cerrando consumidor...")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error Kafka: {msg.error()}")
                    break

            empty_polls = 0
            dataset = 'transmetro_validaciones' if msg.topic() == 'transmetro_topic' else 'aerometro_boardings'
            buffer[dataset].append(json.loads(msg.value().decode('utf-8')))

            if len(buffer[dataset]) >= max_batch_size:
                conteo_total[dataset] += guardar_batch_bronze(buffer[dataset], dataset)
                buffer[dataset] = []

    except KeyboardInterrupt:
        pass
    finally:
        for d_name in buffer:
            if buffer[d_name]:
                conteo_total[d_name] += guardar_batch_bronze(buffer[d_name], d_name)
        consumer.close()

        # Asegurar directorio de resultados
        os.makedirs("results", exist_ok=True)

        # 1. Preparar DataFrame con los conteos
        filas = [{"topico_dataset": k, "registros_ingeridos": v} for k, v in conteo_total.items()]
        df_stream = pd.DataFrame(filas)
        total_eventos = df_stream["registros_ingeridos"].sum() if not df_stream.empty else 0

        # 2. Guardar métricas tabulares (.csv)
        ruta_csv = f"results/fase1_1.1_ingesta_streaming_conteos.csv"
        df_stream.to_csv(ruta_csv, index=False, encoding="utf-8")
        print(f"Métricas de streaming guardadas en: {ruta_csv}")

        # 3. Guardar reporte documental (.md)
        ruta_md = f"results/fase1_1.1_ingesta_streaming_resumen.md"
        with open(ruta_md, "w", encoding="utf-8") as f:
            f.write("# Reporte de Ingesta Streaming (Subfase 1.1)\n\n")
            f.write(f"- **Origen:** Consumidor Kafka (Tópicos Transmetro y Aerómetro)\n")
            f.write(f"- **Destino:** Capa Bronze (Archivos Parquet particionados YYYY/MM/DD)\n")
            f.write(f"- **Total Registros Consumidos:** {total_eventos}\n\n")
            f.write("## Detalle de Registros por Tópico\n\n")
            f.write(df_stream.to_markdown(index=False))
            f.write("\n")

        print(f"Reporte de streaming guardado en: {ruta_md}")


if __name__ == "__main__":
    consumir_hacia_bronze()