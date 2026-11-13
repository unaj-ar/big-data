#!/usr/bin/env bash
# Demo de punta a punta del Tema 4 (lo invoca ./run.sh stream):
#   1. crea el tópico en Kafka
#   2. lanza el job de Spark Structured Streaming en el cluster (en segundo plano)
#   3. publica paquetes AWID con el productor
#   4. deja procesar, detiene el job y muestra lo que quedó en HDFS
set -euo pipefail

TOPIC=awid-packets
# El job termina al quedar ocioso IDLE_SECONDS (tras recibir datos) o al llegar a RUN_SECONDS
RUN_SECONDS=${RUN_SECONDS:-600}
IDLE_SECONDS=${IDLE_SECONDS:-30}
ROWS_PER_FILE=${ROWS_PER_FILE:-5000}
# Umbral de tramas deauth/disassoc por ventana de 10 s. Con splits que contienen ataques
# Deauth/Disas usar el valor por defecto del job (20); con el borrador (tráfico normal) 5.
ALERT_THRESHOLD=${ALERT_THRESHOLD:-5}
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

echo "== 1. Esperando Kafka y creando el tópico $TOPIC (3 particiones)"
until docker exec kafka "$KAFKA_TOPICS" --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do sleep 3; done
# Se recrea el tópico para que cada demo arranque sin mensajes de corridas anteriores
docker exec kafka "$KAFKA_TOPICS" --bootstrap-server kafka:9092 --delete --if-exists --topic "$TOPIC"
while docker exec kafka "$KAFKA_TOPICS" --bootstrap-server kafka:9092 --list | grep -qx "$TOPIC"; do sleep 2; done
docker exec kafka "$KAFKA_TOPICS" --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic "$TOPIC" --partitions 3 --replication-factor 1
docker exec kafka "$KAFKA_TOPICS" --bootstrap-server kafka:9092 --describe --topic "$TOPIC"

echo "== 2. Limpiando salidas previas en HDFS y lanzando Spark Structured Streaming (${RUN_SECONDS}s)"
docker exec master hdfs dfs -rm -r -f -skipTrash /awid/streaming >/dev/null
docker exec -d master bash -c "spark-submit --master spark://master:7077 \
  /opt/work/streaming/awid_streaming.py --run-seconds $RUN_SECONDS --idle-seconds $IDLE_SECONDS --alert-threshold $ALERT_THRESHOLD \
  > /opt/work/output/tema4_streaming.log 2>&1"
sleep 25

echo "== 3. Publicando paquetes AWID en Kafka ($ROWS_PER_FILE por escenario)"
docker exec master python3 /opt/work/streaming/awid_producer.py --rows-per-file "$ROWS_PER_FILE"

echo "== 4. Esperando que termine el job de streaming"
while docker exec master pgrep -f awid_streaming.py >/dev/null; do sleep 5; done

echo "== Resultado en HDFS"
docker exec master hdfs dfs -du -h /awid/streaming
docker exec master hdfs dfs -ls /awid/streaming/raw
docker exec master bash -c "spark-submit --master spark://master:7077 \
  /opt/work/streaming/inspect_streaming_output.py 2>/dev/null > /opt/work/output/tema4_resultados.txt"
docker exec master cat /opt/work/output/tema4_resultados.txt
echo "Log del job: output/tema4_streaming.log | resumen: output/tema4_resultados.txt"
