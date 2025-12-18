# Image officielle Airflow
FROM apache/airflow:2.3.0

# Passage en root pour installer les dépendances
USER root

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    openjdk-11-jdk \
    wget \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Installation de Spark
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Télécharger et installer Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt/ \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && chown -R airflow:root ${SPARK_HOME}

# Retour à l'utilisateur airflow
USER airflow

# Installation des dépendances Python nécessaires
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Installation de PySpark
RUN pip install --no-cache-dir pyspark==3.3.1

# S'assurer que l'utilisateur airflow a les bonnes permissions
USER airflow