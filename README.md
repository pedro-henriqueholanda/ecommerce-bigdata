# Do clique ao Data Warehouse — Pipeline de Big Data para e-commerce

Pipeline que monitora **cliques, carrinho e entregas** de uma varejista online, combinando
processamento em **tempo real (Flink)** e em **lote (Spark)**, com armazenamento em **HDFS, HBase e Hive**.

## Arquitetura

```
 gerador.py ──► events.log ──► Flume (TAILDIR, selector replicating)
                                   │
                ┌──────────────────┴──────────────────┐
                ▼                                     ▼
          Kafka (tópico eventos)               HDFS /data/raw/eventos/dt=AAAA-MM-DD
                │                                     │   (logs brutos)
                ▼                                     ▼
   Flink SQL: watermark 10 s +               Spark (ETL + wide dependencies)
   janelas deslizantes (HOP)                          │
                │                                     ▼
                ▼                              Hive (banco ecommerce)
   HBase: trending_produtos                    eventos_limpos, faturamento_categoria,
          alertas_carrinho                     funil_conversao, abandono_cidade,
                                               status_entregas
```

| Camada | Tecnologia | Arquivo |
|---|---|---|
| Geração | Python 3 (JSON lines, ~20 ev/s com picos de 5x, 10% fora de ordem) | `gerador/gerador.py` |
| Ingestão | Flume: TAILDIR → 2 canais → Kafka sink + HDFS sink | `flume/flume.conf` |
| Streaming | Flink 1.17 (PyFlink/SQL): HOP windows + watermark | `flink/flink_job.py` |
| Batch | Spark 3.5 (RDD + Spark SQL) | `spark/spark_job.py` |
| Armazenamento | HDFS (bruto), HBase (alertas), Hive (DW) | `docker-compose.yml` |

## Como rodar

**Pré-requisitos:** Docker + Docker Compose, ~10 GB de RAM livres para o Docker, conexão boa
(a 1ª execução baixa ~5 GB de imagens). No Windows, rodar dentro do WSL2.

```bash
./scripts/01_subir.sh       # sobe tudo, cria tópico Kafka + tabelas HBase, liga gerador e Flume
./scripts/02_flink.sh       # envia o job de streaming (UI: http://localhost:8081)
# espere 5–10 min acumulando dados
./scripts/03_spark.sh       # batch sobre o dia de hoje (UTC) -> Hive
./scripts/04_verificar.sh   # mostra o dado em cada camada (ótimo para o vídeo)
./scripts/05_derrubar.sh    # desliga tudo
```

Interfaces: HDFS `http://localhost:50070` · Flink `http://localhost:8081` · HBase `http://localhost:16010` · Spark `http://localhost:4040` (enquanto o job roda).

## O que cada job calcula

**Flink (tempo real → HBase)**
- `trending_produtos`: Top-5 produtos mais vistos numa janela de 2 min que desliza a cada 30 s. Rowkey `fimDaJanela#posição`.
- `alertas_carrinho`: usuários com ≥ R$200 no carrinho e nenhuma compra numa janela de 3 min (desliza 30 s). Rowkey `user_id#fimDaJanela`.
- Watermark `event_time - 10 s`: eventos com até 10 s de atraso entram na janela certa; os mais atrasados são descartados.

**Spark (lote → Hive)**, particionado por `dt`, idempotente (re-executar sobrescreve só o dia):
1. `eventos_limpos` — ETL: remove inválidos, **deduplica por event_id**, padroniza cidade/categoria.
2. `faturamento_categoria` — **RDD `reduceByKey`** + join com pedidos por categoria.
3. `funil_conversao` — **groupBy + pivot**: visualização → carrinho → compra.
4. `abandono_cidade` — **left_anti join** entre quem adicionou ao carrinho e quem comprou.
5. `status_entregas` — **Window por order_id** (último status) + joins: pedidos por status/cidade e quantos atrasaram.

O job imprime `rdd.toDebugString()`, onde aparece o `ShuffledRDD` — evidência visual da wide dependency.

## Decisões técnicas (base para o vídeo)

- **Por que Flink e não só Spark Streaming?** Flink processa evento a evento (latência de milissegundos) e foi desenhado em torno de *event time*: watermarks, janelas deslizantes e tratamento de atraso são nativos. O Spark Structured Streaming trabalha em micro-batches (latência de segundos) — bom, mas o Spark foi usado onde ele brilha: processar grandes volumes históricos em lote.
- **Por que HBase e não só HDFS?** HDFS é otimizado para arquivos grandes, escrita sequencial e leitura em varredura; não permite buscar ou atualizar uma linha por chave rapidamente. Os alertas precisam de escrita contínua e consulta pontual ("quais alertas o usuário U00123 teve?") — exatamente o acesso por rowkey do HBase. O rowkey `user_id#janela` agrupa os alertas de um usuário.
- **Por que Kafka entre Flume e Flink?** O Flume não tem um sink que o Flink consiga consumir diretamente. O Kafka é o buffer durável: desacopla ingestão de processamento, permite reprocessar (replay por offset) e é o conector de fonte mais maduro do Flink.
- **Por que Hive?** Camada de Data Warehouse com SQL sobre o HDFS, tabelas particionadas por dia para consultas analíticas baratas.
- **Replicating no Flume:** o mesmo evento vai para o caminho quente (Kafka→Flink) e para o frio (HDFS→Spark) — arquitetura Lambda.
- **Trade-off do watermark:** 10 s = equilíbrio entre latência (resultado sai 10 s depois do fim da janela) e completude (eventos com até 10 s de atraso são contados). O gerador manda atrasos de 2 a 30 s de propósito, então parte é descartada — mostra o custo da escolha.
- **Janelas curtas (2–3 min):** escolhidas para a demonstração caber no vídeo; em produção seriam, por exemplo, 1 h deslizando a cada 5 min.
- **Limitação conhecida:** com janela deslizante, o mesmo carrinho abandonado pode gerar alertas em janelas consecutivas. Em produção usaríamos janela de sessão ou timers por usuário (`KeyedProcessFunction`).

## Roteiro sugerido para o vídeo (5 min)

| Tempo | Conteúdo | O que mostrar na tela |
|---|---|---|
| 0:00–0:30 | Problema e arquitetura | Diagrama deste README |
| 0:30–1:15 | Geração + ingestão | `tail -f data/logs/events.log`, `flume.conf` (replicating), arquivos no HDFS (UI 50070) |
| 1:15–2:30 | Streaming | SQL do `flink_job.py` (WATERMARK e HOP), job rodando na UI 8081, `scan` no HBase |
| 2:30–3:30 | Batch | Rodar `03_spark.sh`, apontar o `ShuffledRDD`, consulta no Hive via beeline |
| 3:30–4:40 | Decisões e trade-offs | Flink × Spark Streaming, HBase × HDFS, Kafka, watermark |
| 4:40–5:00 | Dificuldades e conclusão | O que deu trabalho e como resolveu |

Dica: grave cada trecho separado e junte depois — é mais fácil do que acertar tudo numa tomada.

## Problemas comuns

- **Mac com chip M1/M2/M3:** as imagens de Hadoop/Hive/HBase são amd64 e rodam emuladas (`platform: linux/amd64`); funciona, só fica mais lento. Dê bastante RAM ao Docker Desktop.
- **HDFS preso em safe mode:** `docker exec namenode hdfs dfsadmin -safemode leave`.
- **Flume não inicia (erro com Guava/Hadoop):** veja `docker logs flume`. Se for conflito de Guava, remova o jar do Flume: `docker exec flume sh -c 'rm /opt/flume/lib/guava-*.jar'` e `docker restart flume`.
- **Nada no HBase:** confira em `http://localhost:8081` se o job está RUNNING e se o gerador está ativo (`docker logs gerador`). Os resultados só saem quando o watermark passa do fim da janela (~2,5 min depois de iniciar).
- **Spark diz que o caminho não existe:** o Flume monta `dt=` em UTC e fecha arquivos a cada 60 s. Confira com `docker exec namenode hdfs dfs -ls /data/raw/eventos` e passe a data: `./scripts/03_spark.sh 2026-09-26`.
- **Pouca memória:** pare o `hive-server` enquanto roda o Spark (`docker stop hive-server`) e religue para consultar.

## Estrutura

```
├── docker-compose.yml
├── docker/            # Dockerfiles do Flume e do Flink (+ env do Hadoop/Hive)
├── gerador/gerador.py
├── flume/flume.conf
├── flink/flink_job.py
├── spark/spark_job.py
└── scripts/           # 01_subir, 02_flink, 03_spark, 04_verificar, 05_derrubar
```
