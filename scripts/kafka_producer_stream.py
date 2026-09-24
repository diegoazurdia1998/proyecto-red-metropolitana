import csv
import json
import os
import time
from confluent_kafka import Producer

KAFKA_CONF = {'bootstrap.servers': 'localhost:9092'}
DATOS_RED_DIR = "./scripts/datos_red"


def delivery_report(err, msg):
    if err is not None:
        print(f"Fallo en entrega: {err}")


def simular_streaming():
    producer = Producer(KAFKA_CONF)

    streams = [
        ("transmetro_validaciones.csv", "transmetro_topic"),
        ("aerometro_boardings.csv", "aerometro_topic")
    ]

    for archivo_csv, topic in streams:
        ruta = os.path.join(DATOS_RED_DIR, archivo_csv)
        print(f"Transmitiendo {archivo_csv} a topic {topic}...")

        with open(ruta, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # Si es Aerómetro usa user_hash; si es Transmetro busca tarjeta/num_tarjeta
                if "aerometro" in topic:
                    msg_key = row.get("user_hash", "key")
                else:
                    msg_key = row.get("tarjeta") or row.get("num_tarjeta") or row.get("id_tarjeta") or "key"

                producer.produce(
                    topic,
                    key=str(msg_key),
                    value=json.dumps(row).encode('utf-8'),
                    callback=delivery_report
                )
                count += 1
                if count % 10000 == 0:
                    producer.flush()
                    print(f"[{topic}] {count} eventos enviados...")

            producer.flush()
            print(f"Completado {archivo_csv}: {count} eventos enviados.")


if __name__ == "__main__":
    simular_streaming()