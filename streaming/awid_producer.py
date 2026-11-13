"""Tema 4 - Productor Kafka: reproduce paquetes de AWID3 como si fueran capturados en vivo.

Lee los CSV montados en /data/awid, toma las columnas relevantes y publica cada paquete
como JSON en el tópico `awid-packets` (clave = escenario, para que cada escenario
caiga siempre en la misma partición y conserve el orden).
"""
import argparse
import csv
import glob
import json
import os
import time

from kafka import KafkaProducer

PUBLISHED_COLUMNS = {
    "frame.len": "frame_len",
    "frame.time_epoch": "capture_epoch",
    "wlan.fc.type": "fc_type",
    "wlan.fc.subtype": "fc_subtype",
    "wlan.fc.protected": "fc_protected",
    "wlan.sa": "wlan_sa",
    "wlan.da": "wlan_da",
    "ip.proto": "ip_proto",
    "Label": "label",
}


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="awid-packets")
    parser.add_argument("--data-dir", default="/data/awid")
    parser.add_argument("--rows-per-file", type=int, default=5000, help="0 = archivo completo")
    parser.add_argument("--rate", type=int, default=2000, help="paquetes/segundo (aprox.)")
    return parser.parse_args()


def read_packets(csv_path: str, row_limit: int):
    scenario = os.path.basename(os.path.dirname(csv_path)).split(".", 1)[-1]
    with open(csv_path, encoding="ISO-8859-1", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row_number, row in enumerate(reader):
            if row_limit and row_number >= row_limit:
                break
            packet = {alias: row.get(original, "") for original, alias in PUBLISHED_COLUMNS.items()}
            packet["scenario"] = scenario
            yield scenario, packet


def main():
    arguments = parse_arguments()
    producer = KafkaProducer(
        bootstrap_servers=arguments.bootstrap,
        key_serializer=str.encode,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=50,
    )
    csv_paths = sorted(glob.glob(os.path.join(arguments.data_dir, "*", "*.csv")))
    pause_seconds = 1.0 / arguments.rate if arguments.rate > 0 else 0
    sent_packets = 0
    started_at = time.time()

    for csv_path in csv_paths:
        for scenario, packet in read_packets(csv_path, arguments.rows_per_file):
            producer.send(arguments.topic, key=scenario, value=packet)
            sent_packets += 1
            if pause_seconds and sent_packets % 100 == 0:
                time.sleep(pause_seconds * 100)
        print(f"Publicado {os.path.basename(csv_path)} (acumulado: {sent_packets})", flush=True)

    producer.flush()
    elapsed = time.time() - started_at
    print(f"Total: {sent_packets} paquetes en {elapsed:.1f}s ({sent_packets / elapsed:.0f} pkt/s)", flush=True)


if __name__ == "__main__":
    main()
