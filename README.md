# Trabajo Final Big Data — AWID3 sobre un cluster Hadoop/Spark

Solución portable: corre en cualquier máquina **Linux (amd64 o arm64) o macOS con Docker**.
Todo (Hadoop, Spark, Kafka, Python, librerías) vive dentro de contenedores; el host solo necesita Docker.

| Tema | Entregable | Dónde |
|---|---|---|
| 1. Infraestructura | Cluster de 3 nodos HDFS + Spark en Docker | `cluster/`, `scripts/load_awid_to_hdfs.sh` |
| 2. Ecosistema Hadoop | Apache Hive y Apache HBase | `docs/INFORME.md` §2 |
| 3. Procesamiento | Job PySpark (DataFrames + RDD) con métricas y gráficos | `jobs/awid_processing.py` → `output/tema3/` |
| 4. Integración | Kafka → Spark Structured Streaming → HDFS | `streaming/` → `output/tema4_*` |
| 5. Papers | Análisis de 3 artículos | `docs/INFORME.md` §5 |

El informe completo está en **[`docs/INFORME.md`](docs/INFORME.md)**.

## Requisitos de la máquina destino

- Docker Engine 20.10+ y Docker Compose v2 (`docker compose version`)
- 8 GB de RAM libres para Docker (3 nodos × 2 GB + Kafka 1 GB), 4 cores, ~5 GB de disco
- Acceso a internet en el primer `up` (descarga Hadoop, Spark y conectores al construir la imagen)

Instalación de Docker en Ubuntu/Debian (si no está):

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"   # cerrar sesión y volver a entrar
```

## Puesta en marcha (máquina nueva)

```bash
# 1. Copiar el proyecto (incluye o no el dataset)
scp -r BIGDATA usuario@servidor:~/       # o git clone / unzip

# 2. Indicar dónde están los CSV de AWID3 (si no están en BIGDATA/AWID/csv)
cd ~/BIGDATA
cp cluster/.env.example cluster/.env
nano cluster/.env                         # AWID_DATA_DIR=/ruta/a/AWID3/csv

# 3. Ejecutar (o `chmod +x run.sh` y usar ./run.sh)
bash run.sh up        # construye la imagen y levanta master + worker1 + worker2 (~5 min la 1ª vez)
bash run.sh load      # Tema 1: copia AWID a HDFS (replicación 2, bloques 64 MB)
bash run.sh process   # Tema 3: procesamiento Spark + gráficos  -> output/tema3/
bash run.sh stream    # Tema 4: Kafka -> Spark Streaming -> HDFS -> output/tema4_resultados.txt
bash run.sh status    # estado de DataNodes y Spark workers
bash run.sh down      # detener (conserva HDFS)   |  bash run.sh clean  (borra HDFS)
```

El dataset se monta **solo lectura** en el master. La estructura esperada es la de AWID3-CSV:
`<N.Ataque>/<Ataque>_<k>.csv` (p. ej. `1.Deauth/Deauth_0.csv`, `1.Deauth/Deauth_1.csv`, ...).
Se pueden agregar más splits sin tocar código: `bash run.sh load && bash run.sh process`.

### Interfaces web

En un servidor remoto, abrir un túnel SSH: `ssh -L 9870:localhost:9870 -L 8080:localhost:8080 -L 18080:localhost:18080 usuario@servidor`

| UI | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark History Server | http://localhost:18080 |
| Aplicación Spark en curso | http://localhost:4040 |

### Ajustes por máquina (`cluster/.env`)

| Variable | Default | Uso |
|---|---|---|
| `AWID_DATA_DIR` | `../AWID/csv` | carpeta del dataset en el host |
| `NODE_MEM_LIMIT` | `2g` | memoria máxima por contenedor |
| `SPARK_WORKER_MEMORY` / `SPARK_WORKER_CORES` | `1536m` / `2` | recursos que cada worker ofrece a Spark |
| `PORT_*` | 9870/8080/4040/18080 | puertos publicados (cambiar si están ocupados) |

Parámetros del streaming: `ROWS_PER_FILE=20000 ALERT_THRESHOLD=20 bash run.sh stream`.

## Estructura

```
BIGDATA/
├── run.sh                      # punto de entrada
├── cluster/                    # Tema 1: imagen, compose y configuración
│   ├── Dockerfile  docker-compose.yml  entrypoint.sh  .env.example
│   └── config/  core-site.xml  hdfs-site.xml  workers  spark-defaults.conf  spark-env.sh
├── scripts/load_awid_to_hdfs.sh
├── jobs/awid_processing.py     # Tema 3
├── streaming/                  # Tema 4: productor, job streaming, demo, inspección
├── docs/INFORME.md             # informe (Temas 1-5)
├── output/                     # resultados generados (CSV, PNG, logs)
└── AWID/                       # dataset (opcional, ver AWID_DATA_DIR)
```

## Problemas comunes

- **`permission denied ... docker.sock`**: agregar el usuario al grupo `docker` o usar `sudo bash run.sh ...`.
- **Contenedores que se reinician / `OOMKilled`**: menos de 8 GB para Docker; bajar `NODE_MEM_LIMIT=1536m` y `SPARK_WORKER_MEMORY=1g`.
- **Puerto ocupado**: cambiar `PORT_*` en `cluster/.env`.
- **SELinux (Fedora/RHEL)**: si el master no puede leer el dataset, agregar `:z` al volumen o ejecutar `chcon -Rt svirt_sandbox_file_t <AWID_DATA_DIR>`.
