#!/usr/bin/env bash
# Sobe toda a infraestrutura, cria tópico/tabelas e liga gerador + Flume.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/logs data/flume
HB="docker exec -i hbase hbase shell -n"

echo ">> Subindo HDFS, Hive, Kafka, HBase, Flink e Spark (a 1a vez demora: baixa/compila imagens)..."
docker compose up -d --build namenode datanode hive-metastore-postgresql hive-metastore \
  hive-server kafka hbase jobmanager taskmanager spark

echo ">> Aguardando HDFS sair do safe mode..."
until docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; do sleep 5; done
docker exec namenode hdfs dfs -mkdir -p /data/raw/eventos /user/hive/warehouse

echo ">> Criando tópico Kafka 'eventos'..."
until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do sleep 3; done
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic eventos --partitions 1 --replication-factor 1

echo ">> Criando tabelas no HBase..."
until echo "status" | $HB >/dev/null 2>&1; do sleep 5; done
for t in trending_produtos alertas_carrinho; do
  echo "create '$t', 'info'" | $HB >/dev/null 2>&1 || true
done
echo "list" | $HB

echo ">> Ligando gerador Python + Flume..."
docker compose up -d --build flume gerador

echo
echo "Pronto! Próximos passos:"
echo "  ./scripts/02_flink.sh      # sobe o job de streaming"
echo "  (espere alguns minutos acumulando dados)"
echo "  ./scripts/03_spark.sh      # roda o batch sobre o dia de hoje"
echo "  ./scripts/04_verificar.sh  # mostra dados em HDFS, Kafka, HBase e Hive"
echo "UIs: HDFS http://localhost:50070 | Flink http://localhost:8081 | HBase http://localhost:16010"
