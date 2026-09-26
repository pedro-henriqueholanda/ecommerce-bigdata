#!/usr/bin/env bash
# Uso: ./scripts/03_spark.sh [AAAA-MM-DD]
# Sem argumento processa HOJE (em UTC, mesmo fuso que o Flume usa para montar dt=...).
# Em "produção" o job rodaria de madrugada sobre o dia anterior (padrão do spark_job.py).
set -euo pipefail
DIA="${1:-$(date -u +%F)}"
docker exec spark /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 1g \
  /opt/jobs/spark_job.py --data "$DIA"
