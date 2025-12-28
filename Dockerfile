FROM apache/airflow:2.7.2

USER root
RUN pip install --no-cache-dir kafka-python mysql-connector-python cassandra-driver pyspark
# Copy dags and plugins will be mounted by compose; keep container lean.
RUN apt-get update -y && \
   apt-get install -y --no-install-recommends openjdk-17-jdk && \
   apt-get clean && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --upgrade pip && \
   pip install --no-cache-dir -r /requirements.txt && \
   pip install --no-cache-dir pyspark==3.5.1 apache-airflow-providers-mysql==5.6.0
