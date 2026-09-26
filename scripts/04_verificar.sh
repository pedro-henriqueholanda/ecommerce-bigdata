#!/usr/bin/env bash
# Mostra o dado passando por cada camada — útil para gravar o vídeo.
cd "$(dirname "$0")/.."
HB="docker exec -i hbase hbase shell -n"

echo "===== 1) Gerador: últimas linhas do log ====="
tail -n 2 data/logs/events.log

echo; echo "===== 2) Kafka: 2 mensagens do tópico (vindas do Flume) ====="
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic eventos --max-messages 2 --timeout-ms 10000 2>/dev/null

echo; echo "===== 3) HDFS: logs brutos gravados pelo Flume ====="
docker exec namenode hdfs dfs -ls -R /data/raw/eventos | tail -n 8

echo; echo "===== 4) HBase: trend topics mais recentes (Flink) ====="
echo "scan 'trending_produtos', {LIMIT => 5, REVERSED => true}" | $HB
echo; echo "===== 5) HBase: alertas de carrinho abandonado (Flink) ====="
echo "count 'alertas_carrinho'" | $HB
echo "scan 'alertas_carrinho', {LIMIT => 3}" | $HB

echo; echo "===== 6) Hive: Data Warehouse (Spark) ====="
docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -e "
SHOW TABLES IN ecommerce;
SELECT categoria, faturamento, pedidos FROM ecommerce.faturamento_categoria ORDER BY faturamento DESC;
SELECT cidade, usuarios_com_carrinho, usuarios_abandonaram, taxa_abandono FROM ecommerce.abandono_cidade;
"
