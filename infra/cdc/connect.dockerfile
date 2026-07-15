FROM quay.io/debezium/connect:3.5.2.Final

USER root
RUN rm -rf /kafka/connect/debezium-connector-sqlserver \
           /kafka/connect/debezium-connector-jdbc \
           /kafka/connect/debezium-connector-mysql \
           /kafka/connect/debezium-connector-ibmi \
           /kafka/connect/debezium-connector-vitess \
           /kafka/connect/debezium-connector-mongodb \
           /kafka/connect/debezium-connector-db2 \
           /kafka/connect/debezium-connector-mariadb \
           /kafka/connect/debezium-connector-cockroachdb \
           /kafka/connect/debezium-connector-informix \
           /kafka/connect/debezium-connector-spanner \
           /kafka/connect/debezium-connector-oracle
USER kafka