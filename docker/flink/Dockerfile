# Flink 1.17 + PyFlink + conectores Kafka e HBase
FROM flink:1.17.2-scala_2.12-java11

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip wget \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && pip3 install --no-cache-dir apache-flink==1.17.2 \
 && rm -rf /var/lib/apt/lists/*

RUN wget -q -P /opt/flink/lib \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hbase-2.2/1.17.2/flink-sql-connector-hbase-2.2-1.17.2.jar
