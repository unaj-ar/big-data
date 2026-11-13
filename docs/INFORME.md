# Trabajo Final — Big Data
## Maestría en Ciencia de Datos

**Dataset de referencia:** AWID3-CSV (Aegean Wi-Fi Intrusion Dataset, versión 3)
**Repositorio:** estructura y ejecución en [`README.md`](../README.md)

> Estado del borrador: los resultados numéricos corresponden al primer split de 50.000 paquetes de cada uno de los 13 escenarios (650.000 paquetes, 671 MB). Todo el código está preparado para procesar el dataset completo (≈ 36,9 M de paquetes) sin cambios: basta con agregar los demás splits y volver a ejecutar `bash run.sh load && bash run.sh process`.

---

## 0. El dataset AWID3

AWID3 (Chatzoglou, Kambourakis y Kolias, 2021) es un dataset público de tráfico **IEEE 802.11 (Wi-Fi) capturado en un entorno empresarial real** con autenticación WPA2-Enterprise, 802.11w (Protected Management Frames) y clientes heterogéneos (portátiles, smartphones, IoT). Se distribuye en pcap y en una versión CSV con **254 columnas**: 253 características extraídas con `tshark` de las capas MAC (radiotap, `wlan.*`, EAPOL) y de aplicación (IP, TCP/UDP, DNS, HTTP, TLS, SSH…) y una etiqueta `Label`.

Contiene **13 escenarios de captura**, uno por ataque. En cada escenario, la mayoría del tráfico es normal y una parte es maliciosa:

| Familia | Ataques |
|---|---|
| Capa 2 (802.11) | Deauth, Disas, (Re)Assoc, Rogue AP, Krack, Kr00k, Evil Twin |
| Capa 3+ (aplicación) | SSH (fuerza bruta), Botnet, Malware, SQL Injection, SSDP amplification, Website spoofing |

Totales según el `read_me.pdf` del dataset: **30.387.099 paquetes normales y 6.526.404 maliciosos**. El tráfico malicioso se concentra en SSDP (5,4 M) y Website spoofing (405 k).

Consideraciones de preprocesamiento que indica el propio dataset y que se aplican en el código:

- Cada CSV fue dividido en archivos de 50.000 filas (`split.sh`), y las filas maliciosas fueron etiquetadas manualmente.
- Algunas celdas contienen **varios valores concatenados con “-”**, por ejemplo `radiotap.dbm_antsignal = "-36-41-36"` (una señal por antena) o `ip.proto = "6-6"` (túneles). El código toma el primer valor.
- Hay filas mal formadas por payloads que excedieron el tamaño de celda; se descartan (`mode=DROPMALFORMED`).
- El TLS no siempre está descifrado; las columnas de aplicación tienen muchos nulos.

---

## 1. Tema 1 — Infraestructura: cluster virtual de 3 nodos con HDFS y Spark

### 1.1 Arquitectura

Se virtualizan 3 nodos con **Docker**, cada uno un contenedor con su propio hostname, en una red bridge privada (`bigdata`). Todos usan la misma imagen; el rol se elige con la variable `NODE_ROLE`.

```
                         red docker "bigdata"
 ┌─────────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
 │ master                  │   │ worker1              │   │ worker2              │
 │  HDFS NameNode   :9000  │◄──┤  HDFS DataNode       │   │  HDFS DataNode       │
 │  NameNode UI     :9870  │   │  Spark Worker 2c/1.5G│   │  Spark Worker 2c/1.5G│
 │  Spark Master    :7077  │◄──┤                      │   │                      │
 │  Spark Master UI :8080  │   └──────────────────────┘   └──────────────────────┘
 │  History Server  :18080 │        (bloques HDFS replicados en ambos workers)
 │  driver (spark-submit)  │
 └─────────────────────────┘
       ▲ /data/awid (dataset del host, solo lectura)
```

| Componente | Versión |
|---|---|
| Imagen base | `eclipse-temurin:11-jre-jammy` (Ubuntu 22.04 + Java 11, multi-arquitectura amd64/arm64) |
| Hadoop (HDFS) | 3.3.6 |
| Spark (standalone) | 3.5.3 (build `hadoop3`) |
| Python / PySpark | 3.10, pandas 2.2, matplotlib 3.9 |
| Kafka (Tema 4) | 3.7.0 en modo KRaft (sin ZooKeeper) |

Se eligió **Spark standalone** en lugar de YARN porque es más liviano para 3 nodos con 2 GB cada uno, y la integración con HDFS es la misma (`fs.defaultFS`).

### 1.2 Configuraciones realizadas

**`core-site.xml`**: dirección del sistema de archivos por defecto. Todos los nodos y clientes resuelven HDFS contra el master.

```xml
<property><name>fs.defaultFS</name><value>hdfs://master:9000</value></property>
<property><name>hadoop.tmp.dir</name><value>/hadoop/tmp</value></property>
```

**`hdfs-site.xml`**:

| Propiedad | Valor | Motivo |
|---|---|---|
| `dfs.replication` | `2` | 2 DataNodes: cada bloque se guarda en ambos workers (tolera la caída de uno) |
| `dfs.blocksize` | `64 MB` | los CSV de AWID pesan 35–100 MB, así que cada archivo se reparte en 1–2 bloques y Spark paraleliza mejor |
| `dfs.namenode.name.dir` / `dfs.datanode.data.dir` | `/hadoop/dfs/name`, `/hadoop/dfs/data` | montados en volúmenes Docker persistentes (`namenode`, `datanode1`, `datanode2`) |
| `dfs.namenode.datanode.registration.ip-hostname-check` | `false` | en Docker las IPs internas cambian; los DataNodes se registran por hostname |
| `dfs.client.use.datanode.hostname` / `dfs.datanode.use.datanode.hostname` | `true` | el cliente conecta a `worker1`/`worker2` por nombre DNS de la red Docker |
| `dfs.permissions.enabled` | `false` | entorno académico de un solo usuario |

**`workers`**: lista de nodos esclavos (`worker1`, `worker2`).

**`spark-env.sh`**: recursos que ofrece cada worker. Se parametrizan desde `cluster/.env` para adaptarse a la máquina.

```bash
SPARK_WORKER_CORES=2   SPARK_WORKER_MEMORY=1536m   SPARK_MASTER_HOST=master
```

**`spark-defaults.conf`**:

```properties
spark.master                   spark://master:7077
spark.hadoop.fs.defaultFS      hdfs://master:9000
spark.executor.memory          1g
spark.driver.memory            1g
spark.sql.shuffle.partitions   8        # default 200: excesivo para 4 cores
spark.eventLog.enabled         true
spark.eventLog.dir             hdfs://master:9000/spark-logs   # historial en HDFS
```

**`entrypoint.sh`**: arranca los demonios según el rol.

- *master*: formatea el NameNode solo en el primer arranque, inicia el `namenode` y el `spark master`, crea `/spark-logs` en HDFS y arranca el History Server.
- *worker*: espera a que el NameNode (`master:9000`) y el Spark Master (`master:7077`) respondan, e inicia `datanode` y `spark worker`.

**`docker-compose.yml`**: define los 3 servicios, los volúmenes persistentes, la red, los límites de memoria (`mem_limit: 2g`) y los puertos de las UIs. Monta el dataset (`AWID_DATA_DIR`) en el master **solo lectura**. Kafka queda bajo el *profile* `streaming`, así que solo se levanta para el Tema 4.

**Portabilidad**: todas las rutas son relativas al proyecto y los recursos y puertos se toman de `cluster/.env`. La imagen base es multi-arquitectura, por lo que el mismo `bash run.sh up` funciona en un servidor Linux x86-64 y en ARM (probado en Apple Silicon).

### 1.3 Despliegue

```bash
bash run.sh up       # docker compose up -d --build  + espera a 2 DataNodes y 2 workers vivos
bash run.sh status
```

Verificación (salida real):

```
Live datanodes (2):
Name: 172.20.0.3:9866 (worker1.awid-bigdata_bigdata)
Name: 172.20.0.4:9866 (worker2.awid-bigdata_bigdata)
Master: Registering worker 172.20.0.4:45129 with 2 cores, 1536.0 MiB RAM
Master: Registering worker 172.20.0.3:35219 with 2 cores, 1536.0 MiB RAM
```

### 1.4 Caso de uso: data lake de tráfico Wi-Fi en HDFS + procesamiento distribuido

**Problema.** Un equipo de seguridad necesita almacenar de forma tolerante a fallos las capturas Wi-Fi de una empresa (cientos de GB en producción) y analizarlas en paralelo.

**Paso 1: ingesta en HDFS** (`bash run.sh load` → `scripts/load_awid_to_hdfs.sh`). Se copian los CSV a `/awid/raw/<escenario>/` conservando la organización por escenario:

```
670.8 M  1.3 G  /awid/raw          ← 670,8 MB lógicos ocupan 1,3 GB físicos (replicación ×2)
Live datanodes (2)
blk_1073741829 len=38582340 Live_repl=2 [worker2:9866, worker1:9866]
Average block replication: 2.0
```

`hdfs fsck` confirma que **cada bloque tiene 2 réplicas, una en cada worker**. Si un worker cae, los datos siguen disponibles.

**Paso 2: procesamiento distribuido** (`bash run.sh process`). El driver corre en el master y los executors en worker1 y worker2. Spark lee los bloques **desde los DataNodes donde residen** (localidad de datos): `default_parallelism = 4` (2 workers × 2 cores) y el dataset se divide en 7 particiones de entrada. Los resultados se detallan en el Tema 3.

Las UIs muestran el reparto: en la UI del NameNode (`:9870 → Datanodes`) se ven los bloques por nodo, y en la UI del Spark Master (`:8080`) los 2 workers con sus executors. El History Server (`:18080`) conserva el DAG y las tareas de cada job ejecutado.

---

## 2. Tema 2 — Ecosistema Hadoop: Apache Hive y Apache HBase

Se eligen dos tecnologías complementarias no vistas en el curso. **Hive** cubre el análisis SQL por lotes sobre HDFS. **HBase** cubre el acceso de baja latencia por clave. Entre las dos abarcan las dos formas clásicas de consultar un data lake Hadoop.

### 2.1 Apache Hive

**Qué es y para qué sirve.** Hive es un *data warehouse* sobre Hadoop, creado en Facebook (2008) y donado a Apache. Permite consultar con un dialecto SQL (**HiveQL**) datos almacenados en HDFS u object storage, sin moverlos. Sus componentes:

- **Metastore**: catálogo de tablas, esquemas, particiones y ubicación en HDFS, respaldado por una base relacional (MySQL/PostgreSQL). Spark, Presto/Trino e Impala usan este mismo catálogo.
- **HiveServer2**: servidor JDBC/ODBC para clientes (Beeline, herramientas de BI).
- **Motor de ejecución**: traduce HiveQL a un DAG de tareas que corre sobre **Tez**, MapReduce (legado) o Spark.
- **Schema-on-read**: el esquema se aplica al leer, no al escribir. Una *tabla externa* es solo metadata sobre archivos que ya existen.

**Contexto de uso.** Data warehousing y reporting batch sobre volúmenes grandes (TB–PB), procesos ETL/ELT nocturnos, exploración ad hoc por analistas que saben SQL, y como capa de metadatos compartida del data lake.

| Ventajas | Desventajas |
|---|---|
| SQL estándar: lo usan analistas sin programar en Java/Scala | Latencia alta (segundos a minutos): no sirve para consultas interactivas sub-segundo ni OLTP |
| Escala horizontalmente sobre HDFS a PB | UPDATE/DELETE limitados; las tablas ACID exigen ORC y *compactions* |
| Schema-on-read: consulta archivos existentes sin cargarlos | El rendimiento depende mucho del diseño (particiones, *bucketing*, formato) |
| Particionado y formatos columnares (ORC/Parquet) con *predicate pushdown* | El Metastore es un punto central que hay que operar y respaldar |
| Metastore reutilizable por Spark, Trino, Impala | Hoy compite con motores más rápidos (Trino, Spark SQL) y con formatos de tabla abiertos (Iceberg, Delta) |

**Caso de uso con AWID3.** Exponer el data lake del Tema 1 como tablas SQL para el equipo de seguridad:

```sql
-- Tabla externa: no copia datos, apunta a los CSV ya cargados en HDFS
CREATE EXTERNAL TABLE awid_raw (
  frame_len INT, frame_time_epoch DOUBLE, wlan_fc_type INT, wlan_fc_subtype INT,
  wlan_sa STRING, wlan_da STRING, label STRING   -- … resto de columnas
)
PARTITIONED BY (scenario STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 'hdfs://master:9000/awid/raw'
TBLPROPERTIES ('skip.header.line.count'='1');

ALTER TABLE awid_raw ADD PARTITION (scenario='Deauth') LOCATION '/awid/raw/1.Deauth';

-- Optimización: copia columnar ORC particionada (consultas 10-50x más rápidas que CSV)
CREATE TABLE awid_orc STORED AS ORC AS SELECT * FROM awid_raw;

-- ¿Qué MAC emite más tramas de desautenticación (mgmt subtype 12) en cada escenario?
SELECT scenario, wlan_sa, COUNT(*) AS deauth_frames
FROM awid_orc
WHERE wlan_fc_type = 0 AND wlan_fc_subtype = 12
GROUP BY scenario, wlan_sa
ORDER BY deauth_frames DESC
LIMIT 10;
```

El filtro `WHERE scenario='Deauth'` aprovecha la **poda de particiones**: Hive lee solo esa carpeta de HDFS. El mismo resultado se puede consumir desde Spark con `spark.table("awid_orc")` si Spark se configura contra el Metastore.

### 2.2 Apache HBase

**Qué es y para qué sirve.** HBase es una **base de datos NoSQL distribuida, columnar (wide-column), basada en Google Bigtable**, que guarda sus datos en HDFS. Ofrece lecturas y escrituras **aleatorias y de baja latencia (milisegundos) por clave** sobre tablas de miles de millones de filas y millones de columnas. Su modelo:

- Tabla → filas ordenadas lexicográficamente por **row key** → **familias de columnas** (definidas en el esquema) → columnas (dinámicas) → celdas **versionadas por timestamp**.
- **Regiones**: rangos contiguos de row keys que gestionan los **RegionServers**. El **HMaster** asigna regiones y ZooKeeper coordina el cluster.
- **Escritura**: WAL (en HDFS) → MemStore (memoria) → *flush* a **HFiles** inmutables en HDFS → *compactions*.

**Contexto de uso.** Series temporales y telemetría (IoT, métricas), perfiles de usuario, mensajería (Facebook Messenger la usó), índices y sistemas de recomendación. En general, cualquier caso con mucha escritura y acceso puntual por clave o por rango que necesite consistencia fuerte por fila.

| Ventajas | Desventajas |
|---|---|
| Latencia de ms en `get`/`put` por clave sobre tablas enormes | No tiene SQL ni joins nativos (se agrega con Apache Phoenix) |
| Escala lineal agregando RegionServers; *auto-sharding* por regiones | El diseño de la row key es crítico: una mala clave genera *hotspots* |
| Consistencia fuerte a nivel de fila | Sin índices secundarios nativos ni transacciones multi-fila |
| Esquema flexible: columnas dinámicas, celdas versionadas (útil para historia) | Operación compleja: ZooKeeper, compactions, *region splits*, tuning de JVM/GC |
| Integración nativa con HDFS, MapReduce y Spark | Las consultas analíticas (scans completos) son más lentas que con Parquet + Spark/Hive |

**Caso de uso con AWID3.** Construir un **perfil en tiempo real por dispositivo (MAC)** que un IDS o un analista pueda consultar en milisegundos: “¿qué hizo el dispositivo `0c:9d:92:54:fe:30` en los últimos 5 minutos?”.

Diseño de la row key: `<MAC_origen>#<timestamp_invertido>`. Así todos los eventos de un dispositivo quedan contiguos, ordenados del más reciente al más antiguo, y la consulta se resuelve con un único *scan* acotado por prefijo.

```bash
# HBase shell
create 'wifi_device_events', {NAME => 'f', VERSIONS => 1, TTL => 2592000},   # frame: 30 días
                             {NAME => 'a', VERSIONS => 1}                    # alertas

put 'wifi_device_events', '0c:9d:92:54:fe:30#9223370429080000000', 'f:subtype', '12'
put 'wifi_device_events', '0c:9d:92:54:fe:30#9223370429080000000', 'f:len',     '26'
put 'wifi_device_events', '0c:9d:92:54:fe:30#9223370429080000000', 'f:bssid',   'a4:b1:c1:91:4c:72'
put 'wifi_device_events', '0c:9d:92:54:fe:30#9223370429080000000', 'a:rule',    'DEAUTH_FLOOD'

# Últimos eventos del dispositivo: scan por prefijo (milisegundos, independiente del tamaño de la tabla)
scan 'wifi_device_events', {ROWPREFIXFILTER => '0c:9d:92:54:fe:30#', LIMIT => 50}
```

**Encaje con el resto del trabajo.** En la arquitectura del Tema 4, Spark Structured Streaming podría escribir, además de en HDFS/Parquet (análisis histórico), las alertas y el último estado por MAC en HBase (consulta operativa). Es el patrón **Lambda/Kappa**: HDFS + Hive para el análisis batch y HBase como *serving layer* de baja latencia.

### 2.3 Comparación

| | Hive | HBase |
|---|---|---|
| Tipo | Data warehouse SQL (OLAP) | Base NoSQL wide-column (acceso tipo OLTP) |
| Acceso | Scans y agregaciones masivas | Get/put/scan por row key |
| Latencia | Segundos a minutos | Milisegundos |
| Esquema | Schema-on-read, tablas relacionales | Familias fijas, columnas dinámicas |
| Ideal para AWID3 | Reportes y análisis forense histórico | Perfil vivo por dispositivo, alertas consultables |

---

## 3. Tema 3 — Procesamiento en Spark con DataFrames y RDDs

Código: [`jobs/awid_processing.py`](../jobs/awid_processing.py). Se ejecuta en el cluster con `bash run.sh process` y genera tablas, el resumen JSON y los gráficos en `output/tema3/`.

### 3.1 Detalles del dataset elegido

| Atributo | Valor (borrador) |
|---|---|
| Fuente | AWID3-CSV, Universidad del Egeo — `hdfs://master:9000/awid/raw/*/*.csv` |
| Archivos | 13 (primer split de cada escenario) |
| Tamaño | 670,8 MB (1,3 GB físicos en HDFS con replicación 2) |
| Registros | 649.997 paquetes (3 filas mal formadas descartadas) |
| Columnas originales | 254; se usan 14 relevantes para el análisis |
| Etiquetas en el borrador | 649.987 `Normal`, 10 `Website_spoofing` |
| Período | 10/12/2020 – 26/12/2020; cada escenario es una captura de unos 3 minutos |

> **Limitación del borrador.** Los primeros 50.000 paquetes de cada captura son casi todos tráfico normal: los ataques se lanzan más adelante en la grabación. Por eso este borrador **caracteriza el tráfico base de la red** en cada escenario. Con los demás splits, el mismo job agrega automáticamente las métricas por etiqueta y el gráfico de proporción de ataques (`grafico4_proporcion_ataques.png`).

**Columnas utilizadas**

| Columna AWID3 | Alias | Significado |
|---|---|---|
| `frame.len` | `frame_len` | tamaño de la trama en bytes |
| `frame.time_epoch` | `time_epoch` | instante de captura |
| `radiotap.dbm_antsignal` | `signal_dbm` | potencia recibida (dBm) |
| `radiotap.datarate` | `datarate` | tasa PHY (Mbps) |
| `wlan.fc.type` / `wlan.fc.subtype` | `fc_type`, `fc_subtype` | tipo 802.11: 0 gestión, 1 control, 2 datos; subtipo (p. ej. 12 = deauth) |
| `wlan.fc.protected` | `fc_protected` | trama cifrada (WPA2/CCMP) |
| `wlan.fc.retry` | `fc_retry` | retransmisión |
| `wlan.sa` | `wlan_sa` | MAC de origen |
| `wlan.fixed.reason_code` | `reason_code` | motivo de deauth/disassoc |
| `ip.proto`, `tcp.dstport`, `udp.dstport` | `ip_proto`, `dst_port` | protocolo y puerto de destino |
| `Label` | `label` | Normal o nombre del ataque |

**Limpieza y transformación (DataFrames)**

1. Lectura distribuida de todos los CSV con `header`, codificación ISO-8859-1 y `DROPMALFORMED`.
2. Escenario derivado de la ruta HDFS con `input_file_name()` + `regexp_extract`.
3. **Normalización de celdas multivalor**: se toma el primer número con la regex `^(-?\d+(\.\d+)?)` (`"-36-41-36"` → `-36`, `"6-6"` → `6`, `"24.0"` → `24`).
4. Señales fuera de rango físico (−161 dBm, artefactos del driver) → `null`.
5. `is_malicious = label != "Normal"`.
6. `cache()` del DataFrame limpio, que se reutiliza en 6 agregaciones.

Proporción de nulos: `signal_dbm` 51,9 %, `datarate` 24,3 %, `ip_proto` 71,7 %, `dst_port` 71,6 %. Es lo esperable: **solo el 28 % de las tramas transporta IP**. El resto son tramas de control y gestión 802.11 sin capa de red.

**Uso de RDDs.** El histograma de tamaños se calcula con `DataFrame.rdd.map(...).histogram(buckets)`, una acción RDD distribuida: cada executor cuenta sus particiones y el driver solo recibe los 11 contadores.

### 3.2 Métricas y resultados

**Métricas por escenario** (`metricas_por_escenario.csv`)

| Escenario | Tam. medio (B) | Mediana (B) | Señal media (dBm) | % Gestión | % Control | % Datos | % Cifrado | % Retry | MACs origen | Paq/s |
|---|---|---|---|---|---|---|---|---|---|---|
| (Re)Assoc | 539 | 88 | −97,2 | 4,5 | 52,3 | 43,3 | 41,7 | 4,0 | 58 | 259 |
| Botnet | 425 | 88 | −95,9 | 0,1 | 63,1 | 36,8 | 36,7 | 2,1 | 28 | **8.333** |
| Deauth | 207 | 76 | −44,3 | 4,9 | 71,8 | 23,3 | 19,1 | 2,0 | 52 | 245 |
| Disas | 181 | 76 | −46,5 | 4,5 | **78,9** | 16,7 | 14,1 | 0,6 | 70 | 256 |
| Evil_Twin | 494 | 88 | −97,8 | 4,3 | 50,6 | 45,0 | 42,7 | **6,9** | 38 | 269 |
| Kr00k | 459 | 88 | −97,0 | 4,4 | 59,3 | 36,3 | 34,5 | 1,0 | 24 | 260 |
| Krack | **844** | **339** | −41,2 | 2,0 | 27,5 | **70,6** | **70,3** | **37,0** | 48 | 553 |
| Malware | 310 | 86 | −97,5 | 4,4 | 67,3 | 28,3 | 26,1 | 1,9 | 50 | 256 |
| Rogue_AP | 171 | 76 | −46,6 | 4,6 | 76,7 | 18,7 | 16,5 | 1,0 | **76** | 268 |
| SQL_Injection | 273 | 76 | −43,2 | 5,6 | 70,3 | 24,1 | 20,7 | 1,7 | 59 | 224 |
| SSDP | 324 | 84 | −97,3 | 4,4 | 67,1 | 28,5 | 26,9 | 3,2 | 40 | 256 |
| SSH | 357 | 80 | −98,2 | 4,5 | 65,2 | 30,3 | 27,2 | 1,2 | 30 | 263 |
| Website_spoofing | 249 | 76 | −97,4 | 4,9 | 71,4 | 23,7 | 21,0 | 2,8 | 44 | 234 |

**Protocolos IP** (183.977 tramas con IP): TCP 171.725 (93,3 %), UDP 11.486 (6,2 %), IGMP 761, ICMP 5.

**Puertos de destino más frecuentes** (excluidos los efímeros ≥ 49152): 443/HTTPS 41.866, 80/HTTP 7.333, 445/SMB 2.287, 8080 1.787, 1900/SSDP 1.666, 53/DNS 1.579, 137/NetBIOS 1.286.

**Por etiqueta**: los 10 paquetes `Website_spoofing` presentes miden en promedio 130 B, frente a 372 B del tráfico normal, y ninguno es de gestión. La muestra es muy chica para concluir, pero es consistente con las respuestas DNS/HTTP falsificadas del ataque.

### 3.3 Gráficos

**Gráfico 1 — Tipo de trama 802.11 por escenario** (`grafico1_tipos_trama.png`)

![Tipos de trama](../output/tema3/grafico1_tipos_trama.png)

Las tramas de **control** (ACK, RTS/CTS, Block-Ack) dominan casi todas las capturas (50–79 %). Las de **gestión** son siempre una minoría (≈ 4–5 %; 0,1 % en Botnet). Krack es la excepción: 70 % datos.

**Gráfico 2 — Distribución del tamaño de trama** (`grafico2_tamano_trama.png`, escala log)

![Tamaño de trama](../output/tema3/grafico2_tamano_trama.png)

Distribución **bimodal**: un pico de tramas pequeñas de 64–100 B (ACK y control, más de 300 k) y otro de tramas cercanas a la MTU, de 1.400–2.400 B (transferencias TCP/HTTPS con agregación A-MSDU). Hay pocas tramas de tamaño medio.

**Gráfico 3 — Tasa de tráfico y % de tramas cifradas** (`grafico3_tasa_y_cifrado.png`)

![Tasa y cifrado](../output/tema3/grafico3_tasa_y_cifrado.png)

La mayoría de las capturas mantiene unos 250 paquetes/s. **Botnet** (8.333 paq/s: 50.000 paquetes en 6 s) y **Krack** (553 paq/s) son outliers de volumen. El porcentaje cifrado varía entre 14 % (Disas) y 70 % (Krack).

### 3.4 Conclusiones

1. **El tráfico base de una red Wi-Fi está dominado por la capa 2.** El 72 % de las tramas no lleva IP, así que un IDS que mire solo la capa de red (como los IDS cableados clásicos) no ve la mayor parte del tráfico, ni los ataques de gestión (Deauth, Disas, (Re)Assoc), que viajan en ese 72 %.
2. **Las tramas de gestión son escasas en condiciones normales (≈ 4–5 %).** Por eso una inundación de deauth/disassoc es fácil de detectar como anomalía estadística, y esa es la regla que se implementa en streaming en el Tema 4. Con 802.11w activo (como en AWID3), estas tramas deberían estar protegidas, lo que agrega una señal más.
3. **Hay dos “perfiles de red” distintos entre capturas.** Deauth, Disas, Rogue AP, SQL Injection y Krack tienen señal de unos −44 dBm y menos tramas cifradas. Las demás capturas tienen señal de unos −97 dBm. Esto refleja **dos sesiones de captura con distinta posición de la antena o el hardware de monitorización**. Para entrenar un modelo, `signal_dbm` sería una variable de *leakage*: identifica la sesión de captura, no el ataque, y debería excluirse o normalizarse por sesión.
4. **Krack y Botnet son outliers incluso en su tráfico “normal”.** Krack muestra 37 % de retransmisiones y 70 % de tramas de datos cifradas, coherente con la reinstalación de claves y el reenvío de mensajes del 4-way handshake. Botnet tiene un ritmo 30 veces mayor y casi no tiene tramas de gestión, lo que sugiere una ráfaga de tráfico de datos (C&C/descargas) en ese tramo.
5. **El tráfico de aplicación es mayoritariamente HTTPS (el puerto 443 concentra el 61 % del top-10 de puertos de destino)**, lo que limita la inspección de payload. Los ataques de capa 3+ (SQLi, website spoofing) solo pueden detectarse por metadatos o con el descifrado TLS que AWID3 provee de forma parcial.
6. **Desbalance de clases.** En el dataset completo los paquetes maliciosos son el 17,7 % (y la mayoría son SSDP), y en este borrador son el 0,002 %. Un clasificador necesitaría muestreo estratificado por escenario y métricas como F1 o AUC-PR en lugar de *accuracy*.

**Rendimiento:** 650 k filas × 254 columnas procesadas en el cluster (4 cores, 2 executors) en unos 60 s, la mayor parte en el parseo del CSV. Convertir el raw a Parquet en HDFS (como hace el Tema 4) reduciría el tiempo de lectura y el espacio de forma considerable.

---

## 4. Tema 4 — Integración: Kafka + Spark Structured Streaming + HDFS

### 4.1 Arquitectura

```
 CSV AWID3 ──► awid_producer.py ──► Kafka topic "awid-packets" ──► Spark Structured Streaming ──► HDFS
 (simula sensor    (kafka-python,      (3 particiones,                (cluster: master + 2 workers)    /awid/streaming/
  Wi-Fi en vivo)    JSON, key=escenario) KRaft, sin ZooKeeper)          ├─ raw     → Parquet particionado por escenario
                                                                         ├─ metrics → ventanas de 10 s por escenario
                                                                         └─ alerts  → regla IDS deauth/disassoc
```

- **Kafka** desacopla la captura del procesamiento, amortigua picos (como los 8.333 paq/s de Botnet), permite re-procesar desde un offset y escala por particiones. La clave `escenario` garantiza el orden dentro de cada escenario.
- **Spark Structured Streaming** procesa en micro-batches de 10 s sobre el mismo cluster del Tema 1, con *exactly-once* hacia HDFS gracias a los checkpoints.
- **HDFS** es el almacenamiento durable y replicado: el raw en Parquet queda listo para el análisis batch del Tema 3 o para Hive (Tema 2), y las métricas y alertas quedan disponibles para consulta.

### 4.2 Paso a paso de instalación y configuración

Todo está automatizado; estos son los pasos que realizan los scripts.

**1. Imagen Hadoop + Spark con el conector Kafka** (`cluster/Dockerfile`). Además de Hadoop y Spark, se descargan en `$SPARK_HOME/jars` los jars del conector, para no depender de `--packages` (y de internet) al ejecutar:

```
spark-sql-kafka-0-10_2.12-3.5.3.jar   spark-token-provider-kafka-0-10_2.12-3.5.3.jar
kafka-clients-3.4.1.jar                commons-pool2-2.11.1.jar
```

Y la librería Python del productor: `pip install kafka-python-ng`.

**2. Broker Kafka** (servicio `kafka` en `docker-compose.yml`, profile `streaming`). Imagen oficial `apache/kafka:3.7.0` en **modo KRaft**: un solo nodo actúa como broker y controller, sin ZooKeeper.

```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092     # nombre DNS dentro de la red "bigdata"
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1               # un solo broker
KAFKA_NUM_PARTITIONS: 3
KAFKA_HEAP_OPTS: "-Xmx512m -Xms512m"
```

Al estar en la misma red Docker que master y workers, los executors de Spark llegan a `kafka:9092` directamente.

**3. Levantar Kafka y crear el tópico**:

```bash
docker compose --profile streaming up -d kafka
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic awid-packets --partitions 3 --replication-factor 1
```

**4. Job de streaming** (`streaming/awid_streaming.py`), enviado al cluster con `spark-submit --master spark://master:7077`:

- `readStream.format("kafka")` con `subscribe=awid-packets`, `startingOffsets=earliest` y `maxOffsetsPerTrigger=20000` (control de contrapresión).
- `from_json` con el esquema del paquete, más normalización de tipos (igual que en el Tema 3).
- `withWatermark("ingest_time", "1 minute")` para acotar el estado de las ventanas.
- Tres *sinks* con checkpoint en HDFS (`/awid/streaming/_checkpoints/*`):
  - `raw` → `parquet`, `partitionBy("scenario")`, modo append.
  - `metrics` → `groupBy(window(10 s), scenario)` con paquetes, bytes, % gestión, % cifrado, tramas de desconexión y MACs distintas. Modo **update** con `foreachBatch`: cada ventana se versiona con su `batch_id` y al leer se conserva la última versión.
  - `alerts` → las ventanas con `disconnection_frames ≥ umbral`, en JSON.

**5. Productor** (`streaming/awid_producer.py`): lee los CSV y publica cada paquete como JSON a unos 2.000 paq/s.

**6. Ejecución de la demo completa**: `bash run.sh stream`, que ejecuta los pasos 3 a 5 y al final lee HDFS con `streaming/inspect_streaming_output.py`.

### 4.3 Caso de uso: IDS Wi-Fi casi en tiempo real

**Problema.** Detectar inundaciones de tramas *Deauthentication/Disassociation* (ataques Deauth y Disas de AWID3, que expulsan a los clientes de la red o fuerzan la captura del handshake) mientras el tráfico llega, guardando todo el tráfico para análisis forense.

**Regla.** Una trama de desconexión es de gestión (`wlan.fc.type = 0`) con subtipo 10 (disassoc) o 12 (deauth). Si en una ventana de 10 s un escenario supera el umbral, se emite la alerta `POSIBLE ATAQUE DEAUTH/DISASSOC`. Para el borrador (tráfico normal) se usa un umbral de 5, para que la regla se vea en funcionamiento. Con splits que contengan los ataques, el umbral por defecto es 20, porque las inundaciones reales generan cientos de tramas por ventana.

**Resultado de la ejecución** (5.000 paquetes por escenario):

```
Productor:   Total: 65000 paquetes en 54.0s (1203 pkt/s)
Streaming:   Stream ocioso 30s tras 65000 filas: deteniendo

=== Paquetes persistidos en HDFS (raw): 65000 ===            ← sin pérdidas ni duplicados
| scenario   | packets | kafka_partition | deauth_disassoc_frames |
| (Re)Assoc  | 5000    | 2               | 9                      |
| SSDP       | 5000    | 2               | 9                      |
| Krack      | 5000    | 2               | 5                      |
| …          |         |                 |                        |

=== Ventanas de métricas: 19 (cubren 65000 paquetes) ===
| window_start | scenario | packets | bytes   | pct_management | pct_encrypted | disconnection_frames |
| 01:48:20     | (Re)Assoc| 5000    | 729010  | 20.3           | 9.44          | 9                    |
| 01:48:40     | Botnet   | 5000    | 2448821 | 0.08           | 46.2          | 0                    |
| …

=== Alertas generadas: 3 ===
| POSIBLE ATAQUE DEAUTH/DISASSOC | 7 | SSDP      | 01:48:00-01:48:10 |
| POSIBLE ATAQUE DEAUTH/DISASSOC | 9 | (Re)Assoc | 01:48:20-01:48:30 |
| POSIBLE ATAQUE DEAUTH/DISASSOC | 5 | Krack     | 01:48:30-01:48:40 |
```

Resultados completos en `output/tema4_resultados.txt` y el log del job (micro-batches) en `output/tema4_streaming.log`.

**Lecciones de la implementación**

- **Watermark global.** Spark calcula un único watermark con el máximo *event time* de todas las particiones de Kafka. Con un retraso de 20 s, las particiones que avanzaban más lento quedaban detrás del watermark y **sus filas se descartaban como tardías**: solo el 43 % de los paquetes llegaba a las ventanas. Subir el retraso a 1 minuto lo resolvió.
- **Append vs update.** En modo append, una ventana solo se escribe cuando el watermark la cierra. Si el flujo se detiene, las últimas ventanas nunca se emiten. El modo update con `foreachBatch` y versionado por `batch_id` publica resultados en cada micro-batch, que es lo que necesita un IDS.
- **Re-ejecuciones.** El tópico conserva los mensajes (retención), así que la demo recrea el tópico y borra la salida en HDFS antes de cada corrida para no duplicar datos.

### 4.4 Ejemplos hipotéticos que se podrían resolver con Kafka + Spark + HDFS

1. **WIDS corporativo multi-sede.** Sensores en cada oficina publican metadatos 802.11 en Kafka (un tópico por sede). Spark detecta inundaciones de deauth, *Evil Twin* (mismo SSID con BSSID nuevo) o *Rogue AP* (BSSID no inventariado comparado con una tabla maestra en HDFS) y envía alertas a otro tópico consumido por el SIEM. HDFS guarda meses de tráfico para análisis forense.
2. **Detección con modelos de ML en streaming.** Se entrena un modelo (Random Forest o Gradient Boosting) con Spark MLlib sobre el histórico de AWID3 en HDFS y se aplica en streaming con `model.transform()` sobre cada micro-batch. El modelo se reentrena periódicamente con el nuevo tráfico etiquetado.
3. **Monitoreo de calidad de servicio Wi-Fi.** Las mismas métricas por ventana (retransmisiones, señal, tasa PHY, ocupación por canal) sirven para detectar interferencias o APs saturados en un campus o estadio y alimentar un tablero en tiempo real.
4. **Seguridad IoT / OT.** En una planta industrial con sensores Wi-Fi, Kafka recibe la telemetría y Spark detecta dispositivos que cambian de patrón (tasa, destinos, puertos inusuales como 445/SMB), lo que puede indicar movimiento lateral de malware o una botnet.
5. **Analítica de afluencia en comercios o aeropuertos.** A partir de *probe requests* (MAC anonimizadas con hash), Spark calcula en ventanas cuántos dispositivos hay por zona y su tiempo de permanencia, y HDFS guarda la serie histórica para planificación.
6. **Fraude o abuso en hotspots públicos.** Correlacionar eventos de autenticación (EAPOL/RADIUS) por ventana para detectar credenciales compartidas en múltiples dispositivos simultáneos o fuerza bruta.

---

## 5. Tema 5 — Análisis de artículos académicos

Se eligieron tres artículos que forman una cadena lógica con el trabajo. El primero es el **dataset utilizado** (AWID3), el segundo el **antecedente que definió el problema** (AWID original) y el tercero la **tecnología de procesamiento** empleada (Spark).

### 5.1 Chatzoglou, E., Kambourakis, G. y Kolias, C. (2021). *Empirical Evaluation of Attacks Against IEEE 802.11 Enterprise Networks: The AWID3 Dataset*. IEEE Access, 9, 34188–34205. DOI: 10.1109/ACCESS.2021.3061609

**Resumen.** Los autores señalan que los datasets públicos para IDS Wi-Fi estaban obsoletos: el AWID original (2016) usaba WEP y no reflejaba las redes modernas. Construyen un banco de pruebas que replica una **red empresarial con WPA2-Enterprise (802.1X/EAP), 802.11w (Protected Management Frames)** y una mezcla de dispositivos reales (portátiles, smartphones, smart TV, IoT). Sobre esa red ejecutan **13 ataques** que cubren:

- **Capa MAC**: Deauthentication, Disassociation, (Re)Association flooding, Rogue AP, Evil Twin, y los ataques criptográficos **Krack** (reinstalación de claves en el 4-way handshake) y **Kr00k** (CVE-2019-15126, cifrado con clave nula tras la disociación).
- **Capa de aplicación**, lanzados desde dentro de la red: SSH brute force, Botnet, Malware, SQL Injection, SSDP amplification y Website spoofing.

Publican las capturas pcap completas junto con las claves (PMK y TLS) necesarias para descifrarlas, y una versión CSV con 254 características. Como evaluación empírica, analizan el impacto de cada ataque y cuáles características los distinguen. Muestran, por ejemplo, que 802.11w **no evita** todos los ataques de gestión y que los ataques de capa superior requieren características de varias capas.

**Análisis crítico.**

- *Fortalezas*: el tráfico es real y heterogéneo, y la red es representativa de una empresa moderna. Es el primer dataset Wi-Fi con tráfico descifrable (permite estudiar características de aplicación sobre Wi-Fi cifrado) y con ataques recientes (Krack, Kr00k). Las capturas son reproducibles.
- *Debilidades*: cada ataque está en una captura separada, así que **no hay escenarios con ataques mezclados** como ocurriría en producción, y aparecen sesgos de sesión (como el de `signal_dbm` observado en el Tema 3). El etiquetado es parcialmente manual y hubo filas perdidas en la conversión a CSV. Hay un desbalance fuerte (SSDP concentra el 84 % del tráfico malicioso). El escenario está acotado a un solo laboratorio y un solo AP.
- *Relación con este trabajo*: es el dataset analizado. Varias decisiones de preprocesamiento del Tema 3 (celdas multivalor, filas mal formadas, nulos de capa de aplicación) vienen de las particularidades que documentan los autores.

**Principal aporte.** Un **dataset público, moderno y multi-capa para la detección de intrusiones en redes 802.11 empresariales (WPA2-Enterprise + 802.11w)**, con pcaps descifrables y 254 características. Habilita la evaluación de IDS frente a ataques actuales que los datasets anteriores no contemplaban.

### 5.2 Kolias, C., Kambourakis, G., Stavrou, A. y Gritzalis, S. (2016). *Intrusion Detection in 802.11 Networks: Empirical Evaluation of Threats and a Public Dataset*. IEEE Communications Surveys & Tutorials, 18(1), 184–208. DOI: 10.1109/COMST.2015.2402161

**Resumen.** Es el artículo fundacional de la serie AWID y tiene dos partes:

1. Un **survey y una taxonomía de amenazas 802.11**, organizada en ataques de inundación (*flooding*: deauth, disassoc, beacon, probe), de suplantación (*impersonation*: Evil Twin, Rogue AP, Caffe Latte, Hirte) y de inyección (*injection*: ARP injection, Chop-Chop, fragmentación). Para cada ataque analizan el mecanismo y la viabilidad práctica.
2. La publicación del **dataset AWID original** (AWID-CLS y AWID-ATK, en versiones reducida y completa), capturado en una red **WEP** con 155 atributos por trama. Sobre él evalúan algoritmos de aprendizaje automático clásicos (J48, Naive Bayes, Random Forest, OneR, AdaBoost, etc.) con Weka.

Los resultados muestran que los clasificadores detectan bien los ataques de flooding, pero tienen **gran dificultad con los ataques de suplantación**, y que la selección de características (reducir los 155 atributos) mejora la detección y la eficiencia.

**Análisis crítico.**

- *Fortalezas*: fue el primer dataset público grande y etiquetado para IDS Wi-Fi y se convirtió en el estándar de referencia en la literatura (cientos de trabajos lo usan). La taxonomía es clara, y los resultados de base con varios algoritmos permiten comparar trabajos posteriores.
- *Debilidades*: **WEP** estaba obsoleto ya en 2016, por lo que varios ataques del dataset (Chop-Chop, Caffe Latte) no aplican a WPA2/WPA3. Solo incluye características de capa MAC (no de aplicación), y la evaluación usa algoritmos poco optimizados, sin un tratamiento del desbalance de clases. Esas limitaciones motivaron AWID3 (5.1).
- *Relación con este trabajo*: define el problema (IDS a nivel 802.11) y las familias de ataques que siguen vigentes en AWID3. La regla de flooding del Tema 4 se basa en la observación de este paper: los ataques de inundación son los más separables estadísticamente.

**Principal aporte.** Una **taxonomía sistemática de las amenazas 802.11 y el primer dataset público a gran escala (AWID) para evaluar IDS inalámbricos**, con una línea base de algoritmos de ML. Esto estableció un *benchmark* común para el área.

### 5.3 Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S. y Stoica, I. (2016). *Apache Spark: A Unified Engine for Big Data Processing*. Communications of the ACM, 59(11), 56–65. DOI: 10.1145/2934664

**Resumen.** Los autores (creadores de Spark en el AMPLab de UC Berkeley) presentan la motivación, el diseño y la adopción de Spark. Parten de un problema: el ecosistema Hadoop se había fragmentado en sistemas especializados (MapReduce para batch, Storm para streaming, Pregel/Giraph para grafos, Mahout para ML, Hive/Impala para SQL), cada uno con su API y sin compartir datos de forma eficiente. Proponen **un motor unificado** basado en una sola abstracción, los **Resilient Distributed Datasets (RDDs)**:

- Colecciones inmutables y particionadas que se construyen con transformaciones *lazy* (`map`, `filter`, `join`…) y se materializan con acciones.
- **Tolerancia a fallos por linaje (*lineage*)**: en vez de replicar los datos, se registra el grafo de transformaciones y se recalcula solo la partición perdida.
- **Persistencia en memoria** opcional, clave para algoritmos iterativos (ML) y consultas interactivas.

Sobre los RDDs se construyen las librerías **Spark SQL / DataFrames** (con el optimizador Catalyst), **Spark Streaming** (micro-batches), **MLlib** y **GraphX**. Esto permite combinarlas en un mismo programa sin escribir datos intermedios a disco. El artículo muestra que Spark iguala o supera a los sistemas especializados (por ejemplo, el récord del Daytona GraySort 2014, con 100 TB ordenados 3 veces más rápido que Hadoop MapReduce y usando 10 veces menos máquinas) y documenta su adopción en la industria, con más de 1.000 contribuidores.

**Análisis crítico.**

- *Fortalezas*: el argumento de que **la generalidad no implica perder rendimiento** se sostiene con la combinación de RDDs y el planificador de DAG. Las aplicaciones mezclan SQL, ML y streaming sin pegamento, y la API es mucho más productiva que MapReduce. El modelo de fallos por linaje es elegante y barato.
- *Debilidades*: el streaming por micro-batches tiene una latencia mínima del orden de los cientos de milisegundos, mayor que la de motores *record-at-a-time* (Flink). El uso intensivo de memoria exige ajuste fino (particiones, *shuffle*, GC), y el linaje largo en jobs iterativos requiere *checkpointing*. Al ser un artículo escrito por los creadores, carece de una comparación independiente.
- *Relación con este trabajo*: es la base técnica de los Temas 1, 3 y 4. Este trabajo usa precisamente la **unificación** que defiende: el mismo cluster y la misma API ejecutan el procesamiento batch con DataFrames y RDDs (Tema 3) y el streaming desde Kafka (Tema 4, Structured Streaming, evolución de Spark Streaming), leyendo y escribiendo en HDFS. La tolerancia a fallos por checkpoint y linaje es lo que da *exactly-once* a la salida en HDFS.

**Principal aporte.** Demostrar que **una sola abstracción, los RDDs con tolerancia a fallos por linaje y cómputo en memoria, alcanza para unificar batch, SQL, streaming, ML y grafos en un único motor**, con rendimiento comparable o superior al de los sistemas especializados. Esto redefinió la arquitectura del ecosistema Big Data post-MapReduce.

### 5.4 Síntesis

| | AWID (2016) | AWID3 (2021) | Spark (2016) |
|---|---|---|---|
| Tipo | Survey + dataset | Dataset + evaluación empírica | Arquitectura de sistemas |
| Problema | No existía un benchmark para IDS Wi-Fi | El benchmark quedó obsoleto (WEP) | Fragmentación del ecosistema Hadoop |
| Aporte | Taxonomía + primer dataset público | Dataset moderno WPA2-Ent/802.11w, multi-capa | Motor unificado basado en RDDs |
| Uso en este trabajo | Marco del problema, familias de ataque | Datos analizados (Temas 1, 3 y 4) | Motor de procesamiento (Temas 1, 3 y 4) |

Los tres artículos se complementan. Kolias et al. definen **qué detectar**, Chatzoglou et al. proveen **datos realistas para hacerlo hoy** y Zaharia et al. ofrecen **cómo procesarlos a escala**, en batch y en streaming. Ese es el recorrido que implementa este trabajo.

---

## Referencias

1. Chatzoglou, E., Kambourakis, G., & Kolias, C. (2021). Empirical Evaluation of Attacks Against IEEE 802.11 Enterprise Networks: The AWID3 Dataset. *IEEE Access*, 9, 34188–34205.
2. Kolias, C., Kambourakis, G., Stavrou, A., & Gritzalis, S. (2016). Intrusion Detection in 802.11 Networks: Empirical Evaluation of Threats and a Public Dataset. *IEEE Communications Surveys & Tutorials*, 18(1), 184–208.
3. Zaharia, M., et al. (2016). Apache Spark: A Unified Engine for Big Data Processing. *Communications of the ACM*, 59(11), 56–65.
4. Apache Software Foundation. Documentación de Hadoop 3.3.6, Spark 3.5, Kafka 3.7, Hive y HBase. https://hadoop.apache.org, https://spark.apache.org, https://kafka.apache.org, https://hive.apache.org, https://hbase.apache.org
5. Thusoo, A., et al. (2009). Hive: A Warehousing Solution over a Map-Reduce Framework. *PVLDB*, 2(2), 1626–1629.
6. Chang, F., et al. (2006). Bigtable: A Distributed Storage System for Structured Data. *OSDI '06* (modelo en el que se basa HBase).
