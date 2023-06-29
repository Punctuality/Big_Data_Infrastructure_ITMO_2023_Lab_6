FROM bde2020/spark-sbt-template:3.3.0-hadoop3.3

ARG HDFS_ADDRESS

ENV SPARK_APPLICATION_MAIN_CLASS com.github.Punctuality.OpenFoodFactsClustering
ENV SPARK_APPLICATION_ARGS ${HDFS_ADDRESS}