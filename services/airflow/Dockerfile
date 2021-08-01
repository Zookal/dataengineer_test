ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.1.0-python3.6"
FROM ${AIRFLOW_BASE_IMAGE}

USER root

RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean; \
    apt-get -y install default-libmysqlclient-dev

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN pip install --user --no-cache-dir \
    pandas==1.1.4 \
    apache-airflow-providers-apache-spark==2.0.0 \
    apache-airflow-providers-mysql==2.0.0 \
    mysql-connector-python==8.0.11 \
    mysqlclient==2.0.3
