import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    conf = get_spark_app_config()
    spark: SparkSession = SparkSession.builder \
        .config(conf=conf) \
        .config("spark.sql.streaming.schemaInference", "true") \
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

    logger = Log4j(spark)

    raw_df = spark.readStream\
        .format("json") \
        .option("path", "input") \
        .option("maxFilesPerTrigger", "1") \
        .load()
    # .option("cleanSource", "delete") \
    # raw_df.printSchema()

    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                   "CustomerType", "PaymentMethod", "DeliveryType",
                                   "DeliveryAddress.City", "DeliveryAddress.State",
                                   "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    invoice_write_query = flattened_df.writeStream \
        .format("json") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("append") \
        .queryName("Flattened Invoice Writer") \
        .trigger(processingTime="1 minute") \
        .start()

    invoice_write_query.awaitTermination()
