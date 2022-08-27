# Top level build args
ARG build_for=linux/amd64
ARG airflow_version=2.3.2
ARG python_version=3.9

FROM --platform=$build_for apache/airflow:${airflow_version}-python${python_version} as base

ARG SPARK_VERSION="3.1.2"
ARG HADOOP_VERSION="3.2"
ARG OPENJDK_VERSION="11"

ENV APACHE_SPARK_VERSION="${SPARK_VERSION}"
ENV HADOOP_VERSION="${HADOOP_VERSION}"

USER root

# https://github.com/apache/airflow/issues/20911#issuecomment-1025238690
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29 \
  && apt-get update \
  && apt-get install -qy  \
    python3-dev \
    gcc \
    git \
    "openjdk-${OPENJDK_VERSION}-jre-headless" \
    curl \
    less \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

# Configure Spark
RUN curl -s "https://archive.apache.org/dist/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | \
  tar xz -C /opt --owner root --group root --no-same-owner && \
  mv "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" spark

# Fix Spark installation for Java 11 and Apache Arrow library
# see: https://github.com/apache/spark/pull/27356, https://spark.apache.org/docs/latest/#downloading
RUN cp -p "${SPARK_HOME}/conf/spark-defaults.conf.template" "${SPARK_HOME}/conf/spark-defaults.conf" && \
  echo 'spark.driver.extraJavaOptions -Dio.netty.tryReflectionSetAccessible=true' >> "${SPARK_HOME}/conf/spark-defaults.conf" && \
  echo 'spark.executor.extraJavaOptions -Dio.netty.tryReflectionSetAccessible=true' >> "${SPARK_HOME}/conf/spark-defaults.conf"

RUN curl -s "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.874/aws-java-sdk-bundle-1.11.874.jar" -o "/opt/spark/jars/aws-java-sdk-bundle-1.11.874.jar"
# hadoop-aws@3.2.0 has a bug: https://issues.apache.org/jira/browse/HADOOP-16080
RUN rm -rf /opt/spark/jars/hadoop-common-3.2.0.jar \
  /opt/spark/jars/hadoop-client-3.2.0.jar \
  /opt/spark/jars/guava-14.0.1.jar

RUN curl -s "https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar" -o "/opt/spark/jars/guava-27.0-jre.jar"
RUN curl -s "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.2/hadoop-aws-3.2.2.jar" -o "/opt/spark/jars/hadoop-aws-3.2.2.jar"
RUN curl -s "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.2.2/hadoop-client-3.2.2.jar" -o "/opt/spark/jars/hadoop-client-3.2.2.jar"
RUN curl -s "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.2.2/hadoop-common-3.2.2.jar" -o "/opt/spark/jars/hadoop-common-3.2.2.jar"

USER airflow

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
