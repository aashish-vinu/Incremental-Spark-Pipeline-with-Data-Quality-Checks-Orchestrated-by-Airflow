FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q https://dlcdn.apache.org/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz -O /tmp/spark.tgz && \
    tar xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-4.1.0-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

USER airflow

RUN pip install --no-cache-dir \
    pyspark==4.1.0 \
    apache-airflow-providers-apache-spark==4.11.0
