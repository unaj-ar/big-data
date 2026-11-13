#!/usr/bin/env bash
# Punto de entrada único. Requiere solo Docker + Docker Compose v2 en el host (Linux o macOS).
#
#   ./run.sh up          Construye la imagen y levanta el cluster de 3 nodos (HDFS + Spark)
#   ./run.sh status      Muestra nodos HDFS y workers Spark registrados
#   ./run.sh load        Tema 1: carga los CSV de AWID a HDFS
#   ./run.sh process     Tema 3: procesamiento Spark + métricas + gráficos (output/tema3)
#   ./run.sh stream      Tema 4: Kafka -> Spark Structured Streaming -> HDFS
#   ./run.sh logs [nodo] Últimos logs de master, worker1, worker2 o kafka
#   ./run.sh down        Detiene el cluster (conserva datos de HDFS)
#   ./run.sh clean       Detiene y borra volúmenes (HDFS vacío)
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$PROJECT_DIR/cluster"
COMPOSE=(docker compose --project-directory "$CLUSTER_DIR" -f "$CLUSTER_DIR/docker-compose.yml")

ensure_env() {
  [ -f "$CLUSTER_DIR/.env" ] || cp "$CLUSTER_DIR/.env.example" "$CLUSTER_DIR/.env"
  mkdir -p "$PROJECT_DIR/output"
}

# Los archivos generados dentro del contenedor quedan como root; se devuelven al usuario del host.
fix_output_owner() {
  docker exec master chown -R "$(id -u):$(id -g)" /opt/work/output 2>/dev/null || true
}

wait_for_cluster() {
  echo "Esperando que HDFS y Spark registren los 2 workers..."
  for _ in $(seq 1 60); do
    # Durante el arranque estos comandos fallan; se toleran los errores hasta que respondan
    live_datanodes=$( (docker exec master hdfs dfsadmin -report 2>/dev/null || true) \
      | awk -F'[()]' '/Live datanodes/ {print $2}')
    spark_workers=$( (docker exec master curl -s http://master:8080/json/ 2>/dev/null || true) \
      | grep -c '"state" : "ALIVE"' || true)
    if [ "${live_datanodes:-0}" -ge 2 ] && [ "${spark_workers:-0}" -ge 2 ]; then
      echo "Cluster listo: $live_datanodes DataNodes, $spark_workers Spark workers"
      return 0
    fi
    sleep 5
  done
  echo "El cluster no quedó listo a tiempo; revisar: ./run.sh logs" >&2
  exit 1
}

spark_submit() {
  docker exec master spark-submit --master spark://master:7077 "$@"
}

command=${1:-help}
case "$command" in
  up)
    ensure_env
    "${COMPOSE[@]}" up -d --build
    wait_for_cluster
    echo "UIs: HDFS http://localhost:9870 | Spark http://localhost:8080 | History http://localhost:18080"
    ;;
  status)
    docker exec master hdfs dfsadmin -report | grep -E "Live datanodes|^Name:"
    docker exec master curl -s http://master:8080/json/ | grep -E '"(host|state|cores|memory)"'
    ;;
  load)
    docker exec master bash /opt/work/scripts/load_awid_to_hdfs.sh
    ;;
  process)
    spark_submit /opt/work/jobs/awid_processing.py \
      --input hdfs://master:9000/awid/raw --output /opt/work/output/tema3
    fix_output_owner
    ;;
  stream)
    ensure_env
    "${COMPOSE[@]}" --profile streaming up -d kafka
    bash "$PROJECT_DIR/streaming/run_streaming_demo.sh"
    fix_output_owner
    ;;
  logs)
    "${COMPOSE[@]}" logs --tail 100 "${2:-master}"
    ;;
  down)
    "${COMPOSE[@]}" --profile streaming down
    ;;
  clean)
    "${COMPOSE[@]}" --profile streaming down -v
    ;;
  *)
    sed -n '2,11p' "$0"
    ;;
esac
