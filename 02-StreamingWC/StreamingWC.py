import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    conf = get_spark_app_config()
    # spark = SparkSession.builder.appName('Hello Spark').master('local[3]').getOrCreate()
    spark: SparkSession = SparkSession.builder \
        .config(conf=conf) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,"
                                       "org.apache.spark:spark-avro_2.12:3.0.1,"
                                       "io.delta:delta-core_2.12:0.7.0,"
                                       "org.postgresql:postgresql:42.2.19") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties "
                                                 "-Dspark.yarn.app.container.log.dir=app-logs "
                                                 "-Dlogfile.name=hello-spark") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "spark.sql.catalog.spark_catalog")\
        .getOrCreate()
    # # To check configuration
    # conf_out = spark.sparkContext.getConf()
    # print(conf_out.toDebugString())

    logger = Log4j(spark)

    lines_df = spark.readStream\
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # lines_df.printSchema()
    words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
    counts_df = words_df.groupBy("word").count()

    word_count_query = counts_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("complete") \
        .start()

    word_count_query.awaitTermination()
