import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

schema = StructType([
    StructField("crime_id",StringType(),True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True) ,
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType()),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type",StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    """
    creates a readstream based out of a kafka topic, does a simple aggregation and creates a writestream
    Input param: spark session object
    """
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe","police.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option('stopGracefullyOnShutdown', "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")    
    kafka_df.printSchema()
    
    #apply the schema
    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("df"))\
        .select("df.*")
    service_table.printSchema()

    #select original_crime_type_name and disposition
    distinct_table = service_table \
    .select("original_crime_type_name", "disposition") \
    .distinct()
    distinct_table.printSchema()
    
    # count the number of original crime type
    agg_df = distinct_table \
            .select("original_crime_type_name") \
            .groupby("original_crime_type_name") \
            .agg({"original_crime_type_name": "count"})

    # write an output stream
    query = agg_df \
            .writeStream \
            .format("console") \
            .outputMode("complete") \
            .start()

    # attach a ProgressReporter
    query.awaitTermination()
    
    #create a spark dataframe using radio_code.json file. 
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    radio_code_df.printSchema()

    # join the readstream and spark df on disposition column
    join_query = distinct_table \
                .join(radio_code_df,on=["disposition"],how="left") \
                .select("original_crime_type_name","description") \
                .writeStream \
                .trigger(processingTime="10 seconds") \
                .format("console") \
                .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
