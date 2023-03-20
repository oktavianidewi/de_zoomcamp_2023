from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.appName('version-checking').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.version
    print(spark.version)