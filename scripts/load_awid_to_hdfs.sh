#!/usr/bin/env bash
# Caso de uso Tema 1: copia los CSV de AWID3 (montados en /data/awid) a HDFS,
# conservando la carpeta por escenario, y muestra cómo quedan distribuidos los bloques.
# Se ejecuta dentro del contenedor master (lo invoca run.sh).
set -euo pipefail

SOURCE_DIR=/data/awid
HDFS_DIR=/awid/raw

hdfs dfs -mkdir -p "$HDFS_DIR"

find "$SOURCE_DIR" -type f -name '*.csv' | sort | while read -r csv_path; do
  scenario=$(basename "$(dirname "$csv_path")")
  hdfs dfs -mkdir -p "$HDFS_DIR/$scenario"
  echo "-> $scenario/$(basename "$csv_path")"
  hdfs dfs -put -f "$csv_path" "$HDFS_DIR/$scenario/"
done

echo
echo "=== Contenido en HDFS ==="
hdfs dfs -du -s -h "$HDFS_DIR"
hdfs dfs -count -h "$HDFS_DIR"

echo
echo "=== Estado del cluster (DataNodes vivos y capacidad) ==="
hdfs dfsadmin -report | grep -E "Live datanodes|^Name:|DFS Used%|Configured Capacity" || true

echo
echo "=== Bloques y réplicas de un archivo de ejemplo ==="
sample_file=$(hdfs dfs -ls -R "$HDFS_DIR" | awk '/\.csv$/ {print $NF}' | head -1)
hdfs fsck "$sample_file" -files -blocks -locations | grep -E "len=|blk_|Average block replication" | head -10
