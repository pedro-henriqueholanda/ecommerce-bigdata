"""
Job de STREAMING (PyFlink / Flink SQL)

Kafka (tópico 'eventos', alimentado pelo Flume)
  -> event time + WATERMARK (tolera eventos até 10 s fora de ordem)
  -> janelas DESLIZANTES (HOP)
  -> HBase

Saídas:
  1) trending_produtos : Top-5 produtos mais vistos por janela de 2 min que desliza a cada 30 s
                         rowkey = <fim_da_janela>#<posição>
  2) alertas_carrinho  : usuários que colocaram >= R$200 no carrinho e NÃO compraram
                         numa janela de 3 min que desliza a cada 30 s
                         rowkey = <user_id>#<fim_da_janela>

Eventos que chegam depois do watermark (atraso > 10 s) são descartados pela janela.
O gerador manda 10% dos eventos com 2 a 30 s de atraso, então dá para ver as duas situações.

Execução (dentro do container jobmanager):
    flink run -d -py /opt/jobs/flink_job.py
"""
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ZK = os.getenv("HBASE_ZK", "hbase:2181")
ATRASO_MAX_S = os.getenv("WATERMARK_ATRASO_S", "10")

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
cfg = t_env.get_config()
cfg.set("pipeline.name", "ecommerce-streaming")
cfg.set("parallelism.default", "1")
cfg.set("execution.checkpointing.interval", "30 s")
# se a partição ficar sem dados, não trava o watermark
cfg.set("table.exec.source.idle-timeout", "30 s")

# ------------------------------------------------------------------ fonte
t_env.execute_sql(f"""
CREATE TABLE eventos (
    event_id        STRING,
    event_type      STRING,
    user_id         STRING,
    session_id      STRING,
    city            STRING,
    product_id      STRING,
    category        STRING,
    price           DOUBLE,
    quantity        INT,
    order_id        STRING,
    order_total     DOUBLE,
    delivery_status STRING,
    event_time      BIGINT,
    ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR ts AS ts - INTERVAL '{ATRASO_MAX_S}' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'eventos',
    'properties.bootstrap.servers' = '{KAFKA}',
    'properties.group.id' = 'flink-ecommerce',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
)
""")

# ------------------------------------------------------------------ sinks HBase
# Tudo como STRING para ficar legível no `hbase shell` durante a demonstração.
t_env.execute_sql(f"""
CREATE TABLE hbase_trending (
    rowkey STRING,
    info ROW<window_start STRING, window_end STRING, ranking STRING,
             product_id STRING, category STRING, views STRING, add_carrinho STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'trending_produtos',
    'zookeeper.quorum' = '{ZK}'
)
""")

t_env.execute_sql(f"""
CREATE TABLE hbase_alertas (
    rowkey STRING,
    info ROW<user_id STRING, cidade STRING, window_start STRING, window_end STRING,
             itens_adicionados STRING, valor_carrinho STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'alertas_carrinho',
    'zookeeper.quorum' = '{ZK}'
)
""")

# ------------------------------------------------------------------ 1) Trend topics
TRENDING = """
INSERT INTO hbase_trending
SELECT
    CONCAT(DATE_FORMAT(window_end, 'yyyyMMddHHmmss'), '#', CAST(rn AS STRING)),
    ROW(CAST(window_start AS STRING), CAST(window_end AS STRING), CAST(rn AS STRING),
        product_id, category, CAST(views AS STRING), CAST(adds AS STRING))
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY views DESC) AS rn
    FROM (
        SELECT window_start, window_end, product_id, category,
               COUNT(*) FILTER (WHERE event_type = 'view')        AS views,
               COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS adds
        FROM TABLE(HOP(TABLE eventos, DESCRIPTOR(ts), INTERVAL '30' SECOND, INTERVAL '2' MINUTE))
        WHERE product_id IS NOT NULL
        GROUP BY window_start, window_end, product_id, category
    )
)
WHERE rn <= 5
"""

# ------------------------------------------------------------------ 2) Carrinho abandonado
CARRINHO = """
INSERT INTO hbase_alertas
SELECT
    CONCAT(user_id, '#', DATE_FORMAT(window_end, 'yyyyMMddHHmmss')),
    ROW(user_id, city, CAST(window_start AS STRING), CAST(window_end AS STRING),
        CAST(adds AS STRING), CAST(ROUND(valor, 2) AS STRING))
FROM (
    SELECT window_start, window_end, user_id,
           MAX(city) AS city,
           COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS adds,
           COUNT(*) FILTER (WHERE event_type = 'purchase')    AS compras,
           SUM(CASE WHEN event_type = 'add_to_cart' THEN price * quantity ELSE 0.0 END) AS valor
    FROM TABLE(HOP(TABLE eventos, DESCRIPTOR(ts), INTERVAL '30' SECOND, INTERVAL '3' MINUTE))
    GROUP BY window_start, window_end, user_id
)
WHERE adds > 0 AND compras = 0 AND valor >= 200
"""

stmt = t_env.create_statement_set()
stmt.add_insert_sql(TRENDING)
stmt.add_insert_sql(CARRINHO)
resultado = stmt.execute()

cliente = resultado.get_job_client()
print("Job Flink enviado:", cliente.get_job_id() if cliente else "(ver UI em http://localhost:8081)")
