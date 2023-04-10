import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, from_json, to_timestamp, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window

from lib.logger import Log4j
from lib.utils import get_spark_app_config


if __name__ == '__main__':
    conf = get_spark_app_config()
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

    # conf_out = spark.sparkContext.getConf()
    # print(conf_out.toDebugString())

    logger = Log4j(spark)

    stock_schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Type", StringType()),
        StructField("Amount", IntegerType()),
        StructField("BrokerCode", StringType())
    ])

    kafka_df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trades") \
        .option("startingOffsets", "earliest") \
        .load()

    # kafka_df.printSchema()

    value_df = kafka_df.select(
        from_json(col("value").cast("string"), stock_schema).alias("value")
    )

    trade_df = value_df.select("value.*") \
        .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end")) \
        .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

    # trade_df.printSchema()

    # water mark is made to set expiry time
    window_agg_df = trade_df \
        .withWatermark("CreatedTime", "30 minute") \
        .groupBy(
            window(col("CreatedTime"), "15 minute")) \
        .agg(_sum("Buy").alias("TotalBuy"),
             _sum("Sell").alias("TotalSell"))

    # window_agg_df.printSchema()
    output_df = window_agg_df.select("window.start", "window.end", "TotalBuy", "TotalSell")
    """
    # It will be used when want to perform bach processing
    running_total_window = Window.orderBy("end") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    final_output_df = output_df \
        .withColumn("RTotalBuy", _sum("TotalBuy").over(running_total_window)) \
        .withColumn("RTotalSell", _sum("TotalSell").over(running_total_window)) \
        .withColumn("NetValue", expr("RTotalBuy - RTotalSell"))

    final_output_df.show(truncate=False)
    """

    window_query = output_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 Minute") \
        .start()

    logger.info("Waiting for Query")
    window_query.awaitTermination()