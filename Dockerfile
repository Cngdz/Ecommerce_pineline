FROM apache/airflow:3.0.6

USER root
COPY requirements.txt /tmp/requirements.txt
COPY spark-3.5.6-bin-hadoop3.tgz /tmp/spark.tgz

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-3.5.6-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

USER airflow
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

USER root
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -o ${SPARK_HOME}/jars/hadoop-aws.jar && \
    curl -fSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o ${SPARK_HOME}/jars/aws-sdk-bundle.jar

RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar" -o ${SPARK_HOME}/jars/iceberg-spark-runtime.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.77.1/nessie-spark-extensions-3.5_2.12-0.77.1.jar" -o ${SPARK_HOME}/jars/nessie-spark-extensions.jar


RUN JAVA_BIN=$(which java) && \
    JAVA_HOME=$(dirname $(dirname $(readlink -f $JAVA_BIN))) && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment
ENV PATH=$JAVA_HOME/bin:$PATH


# Trở lại airflow user
USER airflow

