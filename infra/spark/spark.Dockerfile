FROM apache/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    tini \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY infra/spark/config/jars/ /opt/spark/jars/
COPY infra/spark/config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY infra/spark/requirements.txt /opt/spark/requirements.txt

RUN pip3 install -r /opt/spark/requirements.txt && \
    pip3 install jupyterlab notebook

RUN mkdir -p /workspace
WORKDIR /app

RUN curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

ENV PYTHONPATH=/app/src

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]