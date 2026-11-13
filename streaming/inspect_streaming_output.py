"""Tema 4 - Lectura batch de lo que el streaming dejó en HDFS (verificación del caso de uso).

Las métricas y alertas se escriben en modo update (una fila por ventana y micro-batch):
se queda la versión con mayor batch_id de cada (ventana, escenario).
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE = "hdfs://master:9000/awid/streaming"


def latest_version(frame):
    window_key = Window.partitionBy("window_start", "scenario").orderBy(F.desc("batch_id"))
    return frame.withColumn("version", F.row_number().over(window_key)) \
        .where("version = 1").drop("version", "batch_id")


spark = SparkSession.builder.appName("AWID3-Tema4-Inspeccion").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

raw = spark.read.parquet(f"{BASE}/raw")
print(f"\n=== Paquetes persistidos en HDFS (raw): {raw.count()} ===")
raw.groupBy("scenario").agg(
    F.count("*").alias("packets"),
    F.first("kafka_partition").alias("kafka_partition"),
    F.sum("is_disconnection").alias("deauth_disassoc_frames"),
).orderBy("scenario").show(20, truncate=False)

metrics = latest_version(spark.read.parquet(f"{BASE}/metrics"))
windowed_packets = metrics.agg(F.sum("packets")).first()[0]
print(f"=== Ventanas de métricas: {metrics.count()} (cubren {windowed_packets} paquetes) ===")
metrics.orderBy("window_start", "scenario").show(40, truncate=False)

alerts = spark.read.json(f"{BASE}/alerts")
if "window_start" in alerts.columns:  # sin alertas, Spark no puede inferir el esquema JSON
    alerts = latest_version(alerts)
    print(f"=== Alertas generadas: {alerts.count()} ===")
    alerts.orderBy("window_start").show(20, truncate=False)
else:
    print("=== Alertas generadas: 0 ===")

spark.stop()
