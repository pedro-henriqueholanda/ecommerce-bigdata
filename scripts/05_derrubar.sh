#!/usr/bin/env bash
# Para tudo. Use "./scripts/05_derrubar.sh -v" para apagar também os dados do HDFS.
cd "$(dirname "$0")/.."
docker compose down "$@"
