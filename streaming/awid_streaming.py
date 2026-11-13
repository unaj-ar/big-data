"""Tema 4 - Spark Structured Streaming: Kafka -> Spark -> HDFS.

Consume el tópico `awid-packets` y ejecuta tres consultas en paralelo:
  1. raw:      guarda cada paquete en HDFS como Parquet particionado por escenario (data lake).
  2. metrics:  métricas por ventana de 10 s y escenario (paquetes, bytes, % management, % cifrado).
  3. alerts:   regla tipo IDS: muchas tramas Deauthentication/Disassociation en una ventana
               (firma de los ataques Deauth/Disas de AWID3) -> alerta guardada en HDFS.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

PACKET_FIELDS = ["frame_len", "capture_epoch", "fc_type", "fc_subtype", "fc_protected",
                 "wlan_sa", "wlan_da", "ip_proto", "label", "scenario"]
PACKET_SCHEMA = StructType([StructField(name, StringType()) for name in PACKET_FIELDS])

# 802.11 management (type 0): subtype 10 = Disassociation, 12 = Deauthentication
DISCONNECTION_SUBTYPES = [10, 12]


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="awid-packets")
    parser.add_argument("--hdfs-base", default="hdfs://master:9000/awid/streaming")
    parser.add_argument("--window", default="10 seconds")
    parser.add_argument("--alert-threshold", type=int, default=20,
                        help="tramas deauth/disassoc por ventana para generar alerta")
    parser.add_argument("--run-seconds", type=int, default=0,
                        help="duración máxima del job (0 = sin límite, modo servicio)")
    parser.add_argument("--idle-seconds", type=int, default=0,
                        help="detener tras N segundos sin datos nuevos, una vez recibido algo (0 = desactivado)")
    return parser.parse_args()


def processed_rows(query) -> int:
    return sum(progress.get("numInputRows", 0) for progress in query.recentProgress)


def await_until_idle(spark: SparkSession, reference_query, idle_seconds: int, max_seconds: int):
    """Espera hasta que el stream quede ocioso (sin filas nuevas) o se alcance la duración máxima.
    El tiempo ocioso permite que Spark ejecute los micro-batches sin datos que cierran las ventanas."""
    import time

    started_at = last_change_at = time.time()
    last_total = 0
    while True:
        spark.streams.awaitAnyTermination(5)
        now = time.time()
        total = processed_rows(reference_query)
        if total != last_total:
            last_total, last_change_at = total, now
        if last_total > 0 and idle_seconds and now - last_change_at >= idle_seconds:
            print(f"Stream ocioso {idle_seconds}s tras {last_total} filas: deteniendo", flush=True)
            return
        if max_seconds and now - started_at >= max_seconds:
            print(f"Duración máxima {max_seconds}s alcanzada: deteniendo", flush=True)
            return


def first_number(column_name: str):
    return F.regexp_extract(F.col(column_name), r"^(-?\d+(?:\.\d+)?)", 1).cast("double")


def read_packets(spark: SparkSession, arguments) -> "DataFrame":
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", arguments.bootstrap)
        .option("subscribe", arguments.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )
    return (
        kafka_stream.select(
            F.from_json(F.col("value").cast("string"), PACKET_SCHEMA).alias("packet"),
            F.col("timestamp").alias("ingest_time"),  # hora de llegada a Kafka (tiempo de evento)
            F.col("partition").alias("kafka_partition"),
        )
        .select("packet.*", "ingest_time", "kafka_partition")
        # El watermark limita el estado de las ventanas (se descartan las de más de 1 minuto).
        # Es global (máximo de todas las particiones), por eso un margen amplio evita perder datos
        # de particiones de Kafka que avanzan más lento.
        .withWatermark("ingest_time", "1 minute")
        .withColumn("frame_len", first_number("frame_len").cast("int"))
        .withColumn("fc_type", first_number("fc_type").cast("int"))
        .withColumn("fc_subtype", first_number("fc_subtype").cast("int"))
        .withColumn("fc_protected", first_number("fc_protected").cast("int"))
        .withColumn("is_disconnection",
                    ((F.col("fc_type") == 0) & F.col("fc_subtype").isin(DISCONNECTION_SUBTYPES)).cast("int"))
    )


def main():
    arguments = parse_arguments()
    spark = SparkSession.builder.appName("AWID3-Tema4-Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    packets = read_packets(spark, arguments)
    base = arguments.hdfs_base

    raw_query = (
        packets.writeStream.format("parquet")
        .option("path", f"{base}/raw")
        .option("checkpointLocation", f"{base}/_checkpoints/raw")
        .partitionBy("scenario")
        .trigger(processingTime="10 seconds")
        .queryName("raw_to_hdfs")
        .start()
    )

    windowed = (
        packets.groupBy(F.window("ingest_time", arguments.window), "scenario")
        .agg(
            F.count("*").alias("packets"),
            F.sum("frame_len").alias("bytes"),
            F.round(F.avg((F.col("fc_type") == 0).cast("double")) * 100, 2).alias("pct_management"),
            F.round(F.avg("fc_protected") * 100, 2).alias("pct_encrypted"),
            F.sum("is_disconnection").alias("disconnection_frames"),
            F.approx_count_distinct("wlan_sa").alias("distinct_sources"),
        )
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "*")
        .drop("window")
    )

    # Modo "update" + foreachBatch (patrón upsert): cada micro-batch escribe las ventanas que
    # cambiaron, con su batch_id. Al leer se conserva la última versión de cada ventana.
    # Así los resultados están disponibles en HDFS casi en tiempo real, sin esperar a que
    # el watermark cierre la ventana (lo que exige el modo "append").
    def write_versioned(output_path: str, output_format: str):
        def write_batch(batch_frame, batch_id):
            batch_frame.withColumn("batch_id", F.lit(batch_id)) \
                .write.mode("append").format(output_format).save(output_path)
        return write_batch

    metrics_query = (
        windowed.writeStream.outputMode("update")
        .foreachBatch(write_versioned(f"{base}/metrics", "parquet"))
        .option("checkpointLocation", f"{base}/_checkpoints/metrics")
        .trigger(processingTime="10 seconds")
        .queryName("window_metrics")
        .start()
    )

    alerts = (
        windowed.where(F.col("disconnection_frames") >= arguments.alert_threshold)
        .withColumn("alert", F.lit("POSIBLE ATAQUE DEAUTH/DISASSOC"))
        .select("window_start", "window_end", "scenario", "disconnection_frames", "packets", "alert")
    )
    alerts_query = (
        alerts.writeStream.outputMode("update")
        .foreachBatch(write_versioned(f"{base}/alerts", "json"))
        .option("checkpointLocation", f"{base}/_checkpoints/alerts")
        .trigger(processingTime="10 seconds")
        .queryName("deauth_alerts")
        .start()
    )

    console_query = (
        windowed.writeStream.outputMode("update").format("console")
        .option("truncate", False).option("numRows", 30)
        .trigger(processingTime="10 seconds")
        .queryName("console")
        .start()
    )

    queries = [raw_query, metrics_query, alerts_query, console_query]
    if arguments.run_seconds or arguments.idle_seconds:
        await_until_idle(spark, raw_query, arguments.idle_seconds, arguments.run_seconds)
        for query in queries:
            print(f"{query.name}: {processed_rows(query)} filas procesadas (últimos batches)", flush=True)
            query.stop()
    else:
        spark.streams.awaitAnyTermination()
    spark.stop()


if __name__ == "__main__":
    main()
