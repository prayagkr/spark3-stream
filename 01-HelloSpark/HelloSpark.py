import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == '__main__':
    conf = get_spark_app_config()
    # spark = SparkSession.builder.appName('Hello Spark').master('local[3]').getOrCreate()
    spark: SparkSession = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    # # To check configuration
    # conf_out = spark.sparkContext.getConf()
    # print(conf_out.toDebugString())

    logger = Log4j(spark)
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info('Starting HelloSpark')
    logger.warn('Starting HelloSpark')

    survey_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    logger.warn(count_df.collect())

    # input("Enter value")
    logger.warn('Finished HelloSpark')
    logger.info('Finished HelloSpark')
    spark.stop()
