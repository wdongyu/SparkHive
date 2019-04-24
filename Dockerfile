FROM registry.njuics.cn/wdongyu/spark:2.4.0

RUN mkdir -p /opt/spark/jars

COPY target/SparkHive-1.0-SNAPSHOT.jar /opt/spark/jars