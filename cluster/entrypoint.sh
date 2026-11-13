#!/usr/bin/env bash
# Arranca los demonios según el rol del nodo:
#   master  -> HDFS NameNode + Spark Master + Spark History Server
#   worker  -> HDFS DataNode + Spark Worker
set -euo pipefail

wait_for_port() {
  local host=$1 port=$2
  until nc -z "$host" "$port"; do
    echo "Esperando $host:$port ..."
    sleep 2
  done
}

case "${NODE_ROLE:?NODE_ROLE debe ser master o worker}" in
  master)
    if [ ! -d /hadoop/dfs/name/current ]; then
      echo "Formateando NameNode (primer arranque)"
      hdfs namenode -format -force -nonInteractive awid-cluster
    fi
    hdfs --daemon start namenode
    wait_for_port master 9000
    "$SPARK_HOME"/sbin/start-master.sh
    # El History Server necesita el directorio de logs en HDFS (requiere al menos un DataNode)
    until hdfs dfs -mkdir -p /spark-logs 2>/dev/null; do sleep 3; done
    "$SPARK_HOME"/sbin/start-history-server.sh
    ;;
  worker)
    wait_for_port master 9000
    hdfs --daemon start datanode
    wait_for_port master 7077
    "$SPARK_HOME"/sbin/start-worker.sh spark://master:7077
    ;;
  *)
    echo "NODE_ROLE desconocido: $NODE_ROLE" >&2
    exit 1
    ;;
esac

# Mantener el contenedor vivo mostrando los logs de los demonios
sleep 5
exec tail -F "$HADOOP_HOME"/logs/*.log "$SPARK_HOME"/logs/*.out
