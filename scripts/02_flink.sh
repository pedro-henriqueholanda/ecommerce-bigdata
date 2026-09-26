#!/usr/bin/env bash
set -euo pipefail
docker exec jobmanager flink run -d -py /opt/jobs/flink_job.py
echo "Acompanhe em http://localhost:8081 — os primeiros resultados aparecem no HBase em ~1 min."
