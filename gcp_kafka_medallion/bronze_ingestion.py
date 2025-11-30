import os
import sys
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp

## Log Config ##
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)] 
)

log = logging.getLogger(__name__)

## Pyspark Session Config ##
def get_spark_session(app_name="Bronze_Kafka_Ingestion"):
    """
    Creates Spark Session with Delta and GCS support.
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        return spark
    except Exception as e:
        log.critical(f"Failed to create Spark Session: {e}")
        sys.exit(1)

## Spark Main Bronze Script
def bronze_main(args):
    spark = get_spark_session()

    ## Subscribing to Kafka Topic
    try:
        df_kafka = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", args.EXTERNAL_IP)\
        .option("subscribe", "wiki-changes")\
        .option("kafka.security.protocol", "PLAINTEXT") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "true") \
        .load()
        
    except Exception as e:
        log.error(f"Kafka Read Failed with Error: {e}")
        sys.exit(1)

    ## Defining Schema + Selecting the Columns from kafka
    df_bronze = df_kafka.select(
        col('topic').cast('string').alias('topic'),
        col('partition').cast('int').alias('partition'),
        col('offset').cast('long').alias('offset'),
        col('timestamp').alias('kafka_timestamp'),
        col('value').cast('string').alias('raw_values')
    ).withColumn('igs_timestamp',current_timestamp())\
    .withColumn('kafka_date',to_date(col('kafka_timestamp')))
    
    ## Spark Write Stream for the Bronze Layer (Micro Batches)
    try:
        query = df_bronze.writeStream\
            .format("delta")\
            .outputMode("append")\
            .option("path",args.landing_path)\
            .option("checkpointLocation",args.checkpoint)\
            .partitionBy('kafka_date')\
            .trigger(availableNow=True)\
            .start()

        query.awaitTermination()
        log.info(f"Data Loaded for the Streaming Set as of {datetime.utcnow()}")
        
    except Exception as e:
        log.error(f"Kafka Write Failed with Error: {e}")
        sys.exit(1)
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion Kafka")
    
    ## Env Variables ##
    parser.add_argument("--EXTERNAL_IP",required=True,type=str,help="Kafka Running Server IP")
    parser.add_argument("--landing_path",required=True,type=str,help="Bronze Landing Place (gs:// --Location)")
    parser.add_argument("--checkpoint",required=True,type=str,help="Bronze Streaming Ingestion Tracking Place (gs:// --Location)")
    
    args = parser.parse_args()
    
    bronze_main(args)
