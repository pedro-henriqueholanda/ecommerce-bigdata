# Base com Hadoop 2.7.4 já instalado: o sink HDFS do Flume precisa das bibliotecas do Hadoop.
FROM bde2020/hadoop-base:2.0.0-hadoop2.7.4-java8

ENV FLUME_VERSION=1.11.0
ENV FLUME_HOME=/opt/flume
RUN curl -fSL https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz -o /tmp/flume.tgz \
 && tar -xzf /tmp/flume.tgz -C /opt \
 && mv /opt/apache-flume-${FLUME_VERSION}-bin ${FLUME_HOME} \
 && rm /tmp/flume.tgz
ENV PATH=${FLUME_HOME}/bin:${PATH}

CMD ["flume-ng", "agent", "-n", "a1", "-c", "/opt/flume/conf", "-f", "/opt/flume-conf/flume.conf", "-Dflume.root.logger=INFO,console"]
