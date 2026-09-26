"""
Job BATCH (PySpark): ETL + insights de negócio sobre o histórico de UM dia.

HDFS (logs brutos do Flume, /data/raw/eventos/dt=AAAA-MM-DD)
  -> ETL (validação, deduplicação, padronização)
  -> agregações e cruzamentos
  -> Hive (banco `ecommerce`, tabelas particionadas por dt)

Wide dependencies (shuffle) usadas de propósito:
  - dropDuplicates(event_id)                 -> shuffle por event_id
  - RDD reduceByKey                          -> faturamento por categoria
  - groupBy + pivot                          -> funil de conversão
  - join left_anti                           -> abandono de carrinho por cidade
  - Window partitionBy(order_id) + joins     -> situação de entrega de cada pedido

Execução (dentro do container spark):
    spark-submit --master "local[*]" /opt/jobs/spark_job.py --data 2026-09-26
    (sem --data processa o dia anterior)
"""
import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("order_total", T.DoubleType()),
    T.StructField("delivery_status", T.StringType()),
    T.StructField("event_time", T.LongType()),
    T.StructField("event_time_iso", T.StringType()),
])


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default=(date.today() - timedelta(days=1)).isoformat(),
                   help="dia a processar (AAAA-MM-DD). Padrão: ontem")
    p.add_argument("--hdfs", default="hdfs://namenode:8020")
    p.add_argument("--metastore", default="thrift://hive-metastore:9083")
    return p.parse_args()


def salvar(spark, df, tabela, dia):
    """Grava no Hive sobrescrevendo só a partição do dia (idempotente: pode rodar de novo)."""
    nome = f"ecommerce.{tabela}"
    df = df.withColumn("dt", F.lit(dia))
    if spark.catalog.tableExists(nome):
        df.write.mode("overwrite").insertInto(nome)
    else:
        df.write.mode("overwrite").format("parquet").partitionBy("dt").saveAsTable(nome)
    print(f"   -> gravado em {nome} (dt={dia})")


def main():
    a = parse_args()
    spark = (SparkSession.builder
             .appName(f"ecommerce-batch-{a.data}")
             .config("spark.sql.warehouse.dir", f"{a.hdfs}/user/hive/warehouse")
             .config("spark.hadoop.fs.defaultFS", a.hdfs)
             .config("spark.hadoop.hive.metastore.uris", a.metastore)
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.shuffle.partitions", "8")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")

    # ============================================================ EXTRACT
    caminho = f"{a.hdfs}/data/raw/eventos/dt={a.data}"
    print(f"\n== Lendo logs brutos de {caminho}")
    brutos = spark.read.schema(SCHEMA).json(caminho)
    n_brutos = brutos.count()

    # ============================================================ TRANSFORM (ETL)
    eventos = (brutos
               .filter(F.col("event_id").isNotNull()
                       & F.col("event_type").isNotNull()
                       & F.col("event_time").isNotNull())
               .dropDuplicates(["event_id"])                       # wide: shuffle
               .withColumn("city", F.initcap(F.trim("city")))
               .withColumn("category", F.lower(F.trim("category")))
               .withColumn("hora", F.hour(F.from_unixtime(F.col("event_time") / 1000)))
               .cache())
    n_limpos = eventos.count()
    print(f"== ETL: {n_brutos} brutos -> {n_limpos} válidos/únicos "
          f"({n_brutos - n_limpos} descartados)")
    salvar(spark, eventos, "eventos_limpos", a.data)

    compras = eventos.filter((F.col("event_type") == "purchase")
                             & F.col("price").isNotNull() & F.col("quantity").isNotNull())

    # ============================================================ 1) Faturamento (RDD)
    print("\n== 1) Faturamento por categoria (RDD + reduceByKey)")
    rdd = (compras.select("category", "price", "quantity").rdd
           .map(lambda r: (r["category"], (r["price"] * r["quantity"], r["quantity"])))
           .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])))   # wide: shuffle
    linhagem = rdd.toDebugString()
    print((linhagem.decode() if isinstance(linhagem, bytes) else linhagem))  # mostra o ShuffledRDD

    fat = spark.createDataFrame(
        rdd.map(lambda kv: (kv[0], round(kv[1][0], 2), int(kv[1][1]))),
        "categoria string, faturamento double, itens_vendidos long")
    pedidos_cat = (compras.groupBy(F.col("category").alias("categoria"))
                   .agg(F.countDistinct("order_id").alias("pedidos")))
    faturamento = (fat.join(pedidos_cat, "categoria", "left")      # wide: join
                   .withColumn("ticket_medio_item",
                               F.round(F.col("faturamento") / F.col("itens_vendidos"), 2))
                   .orderBy(F.desc("faturamento")))
    faturamento.show(truncate=False)
    salvar(spark, faturamento, "faturamento_categoria", a.data)

    # ============================================================ 2) Funil de conversão
    print("\n== 2) Funil de conversão por categoria (groupBy + pivot)")
    funil = (eventos.filter(F.col("category").isNotNull())
             .groupBy(F.col("category").alias("categoria"))
             .pivot("event_type", ["view", "add_to_cart", "purchase"]).count()
             .na.fill(0)
             .withColumnRenamed("view", "visualizacoes")
             .withColumnRenamed("add_to_cart", "adicoes_carrinho")
             .withColumnRenamed("purchase", "itens_comprados")
             .withColumn("taxa_view_carrinho",
                         F.round(F.col("adicoes_carrinho") / F.col("visualizacoes"), 4))
             .withColumn("taxa_carrinho_compra",
                         F.round(F.col("itens_comprados") / F.col("adicoes_carrinho"), 4)))
    funil.show(truncate=False)
    salvar(spark, funil, "funil_conversao", a.data)

    # ============================================================ 3) Abandono por cidade
    print("\n== 3) Abandono de carrinho por cidade (left_anti join)")
    com_carrinho = (eventos.filter(F.col("event_type") == "add_to_cart")
                    .select("user_id", "city").distinct())
    compradores = compras.select("user_id").distinct()
    abandonaram = com_carrinho.join(compradores, "user_id", "left_anti")   # wide: join
    abandono = (com_carrinho.groupBy("city").agg(F.count("*").alias("usuarios_com_carrinho"))
                .join(abandonaram.groupBy("city").agg(F.count("*").alias("usuarios_abandonaram")),
                      "city", "left")
                .na.fill(0, ["usuarios_abandonaram"])
                .withColumn("taxa_abandono",
                            F.round(F.col("usuarios_abandonaram") / F.col("usuarios_com_carrinho"), 4))
                .withColumnRenamed("city", "cidade")
                .orderBy(F.desc("taxa_abandono")))
    abandono.show(truncate=False)
    salvar(spark, abandono, "abandono_cidade", a.data)

    # ============================================================ 4) Logística
    print("\n== 4) Situação das entregas por cidade (Window + joins)")
    pedidos = (compras.groupBy("order_id")
               .agg(F.first("city").alias("cidade"),
                    F.max("order_total").alias("valor_pedido")))
    atualiz = eventos.filter(F.col("event_type") == "delivery_update")
    w = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
    ultimo_status = (atualiz.withColumn("rn", F.row_number().over(w))  # wide: shuffle
                     .filter("rn = 1")
                     .select("order_id", F.col("delivery_status").alias("status_atual")))
    teve_atraso = (atualiz.groupBy("order_id")
                   .agg(F.max(F.when(F.col("delivery_status") == "atrasado", 1).otherwise(0))
                        .alias("teve_atraso")))
    situacao = (pedidos.join(ultimo_status, "order_id", "left")
                .join(teve_atraso, "order_id", "left")
                .na.fill({"status_atual": "sem_atualizacao", "teve_atraso": 0}))
    entregas = (situacao.groupBy("cidade", "status_atual")
                .agg(F.count("*").alias("pedidos"),
                     F.round(F.sum("valor_pedido"), 2).alias("valor_total"),
                     F.sum("teve_atraso").alias("pedidos_com_atraso"))
                .orderBy("cidade", "status_atual"))
    entregas.show(50, truncate=False)
    salvar(spark, entregas, "status_entregas", a.data)

    print("\n== Tabelas no Hive:")
    spark.sql("SHOW TABLES IN ecommerce").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
