#!/usr/bin/env bash
# Recursos que cada Spark Worker ofrece al master
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-2}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-1536m}
export SPARK_MASTER_HOST=master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
