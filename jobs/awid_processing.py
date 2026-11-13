"""Tema 3 - Procesamiento distribuido de AWID3-CSV con Spark DataFrames.

Lee todos los CSV de HDFS (<input>/<N.Escenario>/*.csv), limpia las columnas
relevantes y calcula métricas por escenario de captura y por etiqueta.
Genera tablas CSV, un resumen JSON y gráficos PNG en <output>.

Funciona igual con 1 split por escenario (borrador) o con todos los splits:
si aparecen filas maliciosas, se agregan las métricas de ataque automáticamente.

Uso (desde el host): ./run.sh process
"""
import argparse
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

# Columnas de AWID3 utilizadas (el CSV completo tiene 254)
SELECTED_COLUMNS = {
    "frame.len": "frame_len",
    "frame.time_epoch": "time_epoch",
    "radiotap.dbm_antsignal": "signal_raw",
    "radiotap.datarate": "datarate",
    "wlan.fc.type": "fc_type",
    "wlan.fc.subtype": "fc_subtype",
    "wlan.fc.protected": "fc_protected",
    "wlan.fc.retry": "fc_retry",
    "wlan.sa": "wlan_sa",
    "wlan.fixed.reason_code": "reason_code",
    "ip.proto": "ip_proto",
    "tcp.dstport": "tcp_dstport",
    "udp.dstport": "udp_dstport",
    "Label": "label",
}

FRAME_TYPE_NAMES = {0: "Gestión", 1: "Control", 2: "Datos"}
IP_PROTOCOL_NAMES = {6: "TCP", 17: "UDP", 1: "ICMP", 2: "IGMP", 58: "ICMPv6"}


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="hdfs://master:9000/awid/raw")
    parser.add_argument("--output", default="/opt/work/output/tema3")
    return parser.parse_args()


def first_number(column_name: str):
    """Algunas celdas traen varios valores unidos con '-' (p. ej. '-36-41-36' o '6-6').
    Se extrae el primer número (con signo opcional)."""
    return F.regexp_extract(F.col(column_name), r"^(-?\d+(?:\.\d+)?)", 1).cast("double")


def load_dataset(spark: SparkSession, input_path: str) -> DataFrame:
    raw = (
        spark.read.option("header", True)
        .option("encoding", "ISO-8859-1")
        .option("mode", "DROPMALFORMED")
        .csv(f"{input_path}/*/*.csv")
        .withColumn("source_file", F.input_file_name())
    )
    selected = raw.select(
        *[F.col(f"`{original}`").alias(alias) for original, alias in SELECTED_COLUMNS.items()],
        "source_file",
    )
    return (
        selected.withColumn("scenario", F.regexp_extract("source_file", r"/\d+\.([^/]+)/[^/]+$", 1))
        .withColumn("frame_len", first_number("frame_len"))
        .withColumn("time_epoch", first_number("time_epoch"))
        .withColumn("signal_dbm", first_number("signal_raw"))
        .withColumn("datarate", first_number("datarate"))
        .withColumn("fc_type", first_number("fc_type").cast("int"))
        .withColumn("fc_subtype", first_number("fc_subtype").cast("int"))
        .withColumn("fc_protected", first_number("fc_protected").cast("int"))
        .withColumn("fc_retry", first_number("fc_retry").cast("int"))
        .withColumn("reason_code", first_number("reason_code").cast("int"))
        .withColumn("ip_proto", first_number("ip_proto").cast("int"))
        .withColumn("dst_port", F.coalesce(first_number("tcp_dstport"), first_number("udp_dstport")).cast("int"))
        # Señales fuera de rango físico (p. ej. -161 dBm) son artefactos del driver
        .withColumn("signal_dbm", F.when(F.col("signal_dbm").between(-100, 0), F.col("signal_dbm")))
        .withColumn("is_malicious", (F.col("label") != "Normal").cast("int"))
        .drop("signal_raw", "tcp_dstport", "udp_dstport", "source_file")
    )


def map_column(column_name: str, mapping: dict, default: str):
    expression = F.lit(default)
    for key, value in mapping.items():
        expression = F.when(F.col(column_name) == key, value).otherwise(expression)
    return expression


def describe_dataset(packets: DataFrame) -> dict:
    total_rows = packets.count()
    label_counts = {row["label"]: row["count"] for row in packets.groupBy("label").count().collect()}
    time_bounds = packets.agg(F.min("time_epoch").alias("start"), F.max("time_epoch").alias("end")).first()
    null_ratio = packets.select(
        *[(F.sum(F.col(name).isNull().cast("int")) / F.lit(total_rows)).alias(name)
          for name in ["frame_len", "signal_dbm", "datarate", "fc_type", "ip_proto", "dst_port"]]
    ).first().asDict()
    return {
        "total_packets": total_rows,
        "scenarios": packets.select("scenario").distinct().count(),
        "label_distribution": label_counts,
        # Las capturas son de días distintos: el rango global no es una duración real
        "first_packet_epoch": time_bounds["start"],
        "last_packet_epoch": time_bounds["end"],
        "null_ratio_selected_columns": {key: round(value, 4) for key, value in null_ratio.items()},
    }


def compute_scenario_metrics(packets: DataFrame) -> DataFrame:
    return (
        packets.groupBy("scenario")
        .agg(
            F.count("*").alias("packets"),
            F.sum("is_malicious").alias("malicious_packets"),
            F.round(F.avg("frame_len"), 1).alias("avg_frame_len"),
            F.expr("percentile_approx(frame_len, 0.5)").alias("median_frame_len"),
            F.round(F.avg("signal_dbm"), 1).alias("avg_signal_dbm"),
            F.round(F.avg((F.col("fc_type") == 0).cast("double")) * 100, 2).alias("pct_management"),
            F.round(F.avg((F.col("fc_type") == 1).cast("double")) * 100, 2).alias("pct_control"),
            F.round(F.avg((F.col("fc_type") == 2).cast("double")) * 100, 2).alias("pct_data"),
            F.round(F.avg("fc_protected") * 100, 2).alias("pct_encrypted"),
            F.round(F.avg("fc_retry") * 100, 2).alias("pct_retry"),
            F.countDistinct("wlan_sa").alias("distinct_sources"),
            F.round(F.max("time_epoch") - F.min("time_epoch"), 1).alias("capture_seconds"),
            F.round(F.count("*") / (F.max("time_epoch") - F.min("time_epoch")), 1).alias("packets_per_second"),
        )
        .orderBy("scenario")
    )


def compute_frame_type_distribution(packets: DataFrame) -> DataFrame:
    return (
        packets.withColumn("frame_type", map_column("fc_type", FRAME_TYPE_NAMES, "Unknown"))
        .groupBy("scenario", "frame_type")
        .count()
        .orderBy("scenario", "frame_type")
    )


def compute_protocol_and_ports(packets: DataFrame) -> tuple:
    protocols = (
        packets.where(F.col("ip_proto").isNotNull())
        .withColumn("protocol", map_column("ip_proto", IP_PROTOCOL_NAMES, "Other"))
        .groupBy("protocol").count().orderBy(F.desc("count"))
    )
    top_ports = (
        packets.where(F.col("dst_port") < 49152)  # se excluyen puertos efímeros
        .groupBy("dst_port").count().orderBy(F.desc("count")).limit(10)
    )
    return protocols, top_ports


def compute_attack_metrics(packets: DataFrame) -> DataFrame:
    """Compara tráfico normal vs malicioso por etiqueta (solo útil si hay ataques)."""
    return (
        packets.groupBy("label")
        .agg(
            F.count("*").alias("packets"),
            F.round(F.avg("frame_len"), 1).alias("avg_frame_len"),
            F.round(F.avg((F.col("fc_type") == 0).cast("double")) * 100, 2).alias("pct_management"),
            F.round(F.avg("fc_protected") * 100, 2).alias("pct_encrypted"),
            F.round(F.avg("signal_dbm"), 1).alias("avg_signal_dbm"),
        )
        .orderBy(F.desc("packets"))
    )


def save_table(dataframe: DataFrame, output_dir: str, name: str):
    pandas_frame = dataframe.toPandas()
    pandas_frame.to_csv(os.path.join(output_dir, f"{name}.csv"), index=False)
    print(f"\n=== {name} ===")
    print(pandas_frame.to_string(index=False))
    return pandas_frame


def plot_frame_types(frame_types, output_dir: str):
    pivot = frame_types.pivot(index="scenario", columns="frame_type", values="count").fillna(0)
    percentages = (pivot.div(pivot.sum(axis=1), axis=0) * 100).sort_values("Datos")
    axis = percentages.plot(kind="barh", stacked=True, figsize=(10, 6), colormap="viridis")
    axis.set_xlabel("% de paquetes")
    axis.set_ylabel("Escenario de captura")
    axis.set_title("AWID3: tipo de trama 802.11 por escenario")
    axis.set_xlim(0, 100)
    axis.grid(axis="x", alpha=0.3)
    axis.legend(title="Tipo de trama", loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "grafico1_tipos_trama.png"), dpi=120)
    plt.close()


def plot_frame_length_histogram(packets: DataFrame, output_dir: str):
    # El histograma se calcula distribuido (RDD.histogram) y solo se trae el resultado al driver
    bucket_edges = [0, 64, 100, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 2400]
    _, counts = packets.where(F.col("frame_len").isNotNull()).select("frame_len").rdd \
        .map(lambda row: row[0]).histogram(bucket_edges)
    labels = [f"{low}-{high}" for low, high in zip(bucket_edges[:-1], bucket_edges[1:])]
    plt.figure(figsize=(10, 5))
    plt.bar(labels, counts, color="steelblue")
    plt.yscale("log")
    plt.grid(axis="y", which="major", alpha=0.3)
    plt.xticks(rotation=45)
    plt.xlabel("Tamaño de trama (bytes)")
    plt.ylabel("Paquetes (escala log)")
    plt.title("AWID3: distribución del tamaño de trama")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "grafico2_tamano_trama.png"), dpi=120)
    plt.close()


def plot_traffic_rate_and_encryption(scenario_metrics, output_dir: str):
    scenario_metrics = scenario_metrics.sort_values("packets_per_second")
    figure, rate_axis = plt.subplots(figsize=(11, 5))
    positions = range(len(scenario_metrics))
    rate_axis.bar(positions, scenario_metrics["packets_per_second"], color="darkorange", label="Paquetes/seg")
    rate_axis.set_yscale("log")  # Botnet es un outlier de un orden de magnitud
    rate_axis.set_ylabel("Paquetes por segundo (escala log)")
    rate_axis.grid(axis="y", alpha=0.3)
    rate_axis.set_xticks(list(positions))
    rate_axis.set_xticklabels(scenario_metrics["scenario"], rotation=45, ha="right")
    encryption_axis = rate_axis.twinx()
    encryption_axis.plot(list(positions), scenario_metrics["pct_encrypted"], color="navy", marker="o",
                         label="% tramas cifradas")
    encryption_axis.set_ylabel("% tramas cifradas")
    encryption_axis.set_ylim(0, 100)
    figure.legend(loc="upper right", bbox_to_anchor=(0.9, 0.88))
    rate_axis.set_title("AWID3: tasa de tráfico y cifrado por escenario")
    figure.tight_layout()
    figure.savefig(os.path.join(output_dir, "grafico3_tasa_y_cifrado.png"), dpi=120)
    plt.close(figure)


def plot_attack_share(packets: DataFrame, output_dir: str):
    attack_share = packets.groupBy("scenario").agg(
        (F.avg("is_malicious") * 100).alias("pct_malicious")).orderBy("scenario").toPandas()
    plt.figure(figsize=(10, 5))
    plt.bar(attack_share["scenario"], attack_share["pct_malicious"], color="firebrick")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("% de paquetes maliciosos")
    plt.title("AWID3: proporción de tráfico malicioso por escenario")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "grafico4_proporcion_ataques.png"), dpi=120)
    plt.close()


def main():
    arguments = parse_arguments()
    os.makedirs(arguments.output, exist_ok=True)
    spark = SparkSession.builder.appName("AWID3-Tema3-Procesamiento").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    packets = load_dataset(spark, arguments.input).cache()
    summary = describe_dataset(packets)
    summary["spark"] = {
        "master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "input_partitions": packets.rdd.getNumPartitions(),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    scenario_metrics = save_table(compute_scenario_metrics(packets), arguments.output, "metricas_por_escenario")
    frame_types = save_table(compute_frame_type_distribution(packets), arguments.output, "tipos_de_trama")
    protocols, top_ports = compute_protocol_and_ports(packets)
    summary["ip_protocols"] = save_table(protocols, arguments.output, "protocolos_ip").to_dict("records")
    summary["top_destination_ports"] = save_table(top_ports, arguments.output, "top_puertos").to_dict("records")
    summary["by_label"] = save_table(compute_attack_metrics(packets), arguments.output,
                                     "metricas_por_etiqueta").to_dict("records")

    plot_frame_types(frame_types, arguments.output)
    plot_frame_length_histogram(packets, arguments.output)
    plot_traffic_rate_and_encryption(scenario_metrics, arguments.output)
    if sum(count for label, count in summary["label_distribution"].items() if label != "Normal") > 0:
        plot_attack_share(packets, arguments.output)

    with open(os.path.join(arguments.output, "resumen.json"), "w", encoding="utf-8") as summary_file:
        json.dump(summary, summary_file, indent=2, ensure_ascii=False, default=str)
    spark.stop()


if __name__ == "__main__":
    main()
