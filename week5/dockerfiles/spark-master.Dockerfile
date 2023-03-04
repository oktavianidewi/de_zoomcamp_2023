FROM spark-base

# -- Runtime

ARG SPARK_MASTER_WEB_UI=8080

EXPOSE ${SPARK_MASTER_WEB_UI} ${SPARK_MASTER_PORT}
CMD bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out