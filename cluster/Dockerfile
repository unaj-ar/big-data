# Imagen única para los 3 nodos del cluster (master, worker1, worker2).
# El rol de cada nodo se define con la variable NODE_ROLE en docker-compose.yml.
FROM eclipse-temurin:11-jre-jammy

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.3
ARG SCALA_BINARY=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG APACHE_MIRROR=https://archive.apache.org/dist
ARG MAVEN_REPO=https://repo1.maven.org/maven2

ENV HADOOP_HOME=/opt/hadoop \
    SPARK_HOME=/opt/spark \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PYSPARK_PYTHON=python3 \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl procps netcat-openbsd python3 python3-pip tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir pandas==2.2.3 matplotlib==3.9.2 kafka-python-ng==2.2.3

# Hadoop (HDFS)
RUN curl -fsSL "$APACHE_MIRROR/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
      | tar -xz -C /opt \
    && mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME \
    && rm -rf $HADOOP_HOME/share/doc

# Spark (standalone) compilado para Hadoop 3
RUN curl -fsSL "$APACHE_MIRROR/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz" \
      | tar -xz -C /opt \
    && mv /opt/spark-$SPARK_VERSION-bin-hadoop3 $SPARK_HOME

# Conector Kafka para Structured Streaming (Tema 4), pre-descargado para no depender de --packages
RUN cd $SPARK_HOME/jars \
    && curl -fsSLO "$MAVEN_REPO/org/apache/spark/spark-sql-kafka-0-10_$SCALA_BINARY/$SPARK_VERSION/spark-sql-kafka-0-10_$SCALA_BINARY-$SPARK_VERSION.jar" \
    && curl -fsSLO "$MAVEN_REPO/org/apache/spark/spark-token-provider-kafka-0-10_$SCALA_BINARY/$SPARK_VERSION/spark-token-provider-kafka-0-10_$SCALA_BINARY-$SPARK_VERSION.jar" \
    && curl -fsSLO "$MAVEN_REPO/org/apache/kafka/kafka-clients/$KAFKA_CLIENTS_VERSION/kafka-clients-$KAFKA_CLIENTS_VERSION.jar" \
    && curl -fsSLO "$MAVEN_REPO/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"

COPY config/core-site.xml config/hdfs-site.xml config/workers $HADOOP_CONF_DIR/
COPY config/spark-defaults.conf config/spark-env.sh $SPARK_HOME/conf/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh $SPARK_HOME/conf/spark-env.sh \
    && echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh \
    && echo "log4j.logger.org.apache.hadoop.util.NativeCodeLoader=ERROR" >> $HADOOP_CONF_DIR/log4j.properties

WORKDIR /opt/work
ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
