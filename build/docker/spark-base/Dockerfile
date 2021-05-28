FROM base
LABEL manteiner="Andre Perez <andre.marcos.perez@gmail.com>"

# -- Layer: Image Metadata

ARG build_date

LABEL org.label-schema.build-date=${build_date}
LABEL org.label-schema.name="Apache Spark Standalone Cluster on Docker - Spark Base Image"
LABEL org.label-schema.description="Spark base image shipped Spark"
LABEL org.label-schema.url="https://github.com/andre-marcos-perez/spark-cluster-on-docker"
LABEL org.label-schema.schema-version="1.0"

# -- Layer: Apache Spark

ARG spark_version
ARG hadoop_version

RUN curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    echo "alias pyspark=/usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/bin/pyspark" >> ~/.bashrc && \
    echo "alias spark-shell=/usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/bin/spark-shell" >> ~/.bashrc && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

RUN curl https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${spark_version}/spark-sql-kafka-0-10_2.12-${spark_version}.jar -o spark-sql-kafka-0-10_2.12-${spark_version}.jar &&  \
    mv spark-sql-kafka-0-10_2.12-${spark_version}.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/

RUN curl https://repo.maven.apache.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/${spark_version}/spark-streaming-kafka-0-10_2.12-${spark_version}.jar -o spark-streaming-kafka-0-10_2.12-${spark_version}.jar &&  \
    mv spark-streaming-kafka-0-10_2.12-${spark_version}.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

RUN apt-get update && apt-get install -y python3-pip && ln -s /usr/bin/pip3 /usr/bin/pip

RUN pip install --upgrade pyspark
RUN pip install pandas
RUN pip install urllib3
RUN pip install DBUtils

# Install dependencies
RUN pyspark --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" 

# -- Runtime

WORKDIR ${SPARK_HOME}