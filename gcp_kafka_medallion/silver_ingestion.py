import sys
import argparse
import logging

## Spark Libraries ##
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp, current_timestamp, row_number, to_date

## Logger Config ##
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)] 
)

log = logging.getLogger(__name__)

## Creating Spark Session ##
def get_spark_session(app_name="silver ingestion upsert logic"): 
    try:
        return (
            SparkSession.builder\
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.shuffle.partitions", "20")\
                .getOrCreate()
        )
    except Exception as e:
        log.critical(f"Failed to create Spark Session: {e}")
        sys.exit(1)
        
        
## Custom Function to pass to forcebatch for streaming + upsert logic ##
def UpsertToSilver(microBatchDF,batchID,args):
    valid_microBatchDF = microBatchDF.filter(
        (col('meta_id').isNotNull()) & (col('corrupt_json').isNull())
        )

    ## Dropping Field that doesnt have meta_id
    if valid_microBatchDF.isEmpty():
        return

    ## Defining Window Function for capturing Lastest Updated Data for Delta Merge
    window_spec = W.partitionBy(col('meta_id')).orderBy(col('kafka_timestamp').desc())

    microBatchDF_deduped = valid_microBatchDF.withColumn(
        "rn", row_number().over(window_spec))\
        .filter(col('rn') == 1)\
        .drop(col('rn'))

    try:
            deltaTable = DeltaTable.forPath(microBatchDF.sparkSession, args.silver_landing_layer)

            deltaTable.alias("target").merge(
                source=microBatchDF_deduped.alias("source"),
                condition="target.meta_id = source.meta_id"
            ).whenMatchedUpdateAll(
            ).whenNotMatchedInsertAll(
            ).execute()

    except (AnalysisException, Exception):
            print(f"Table not found at {args.silver_landing_layer}. Creating new table...")

            try:
                microBatchDF_deduped.write.format("delta")\
                    .partitionBy("kafka_date") \
                    .option("mergeSchema", "true")\
                    .save(args.silver_landing_layer)
            except AnalysisException as e:
                log.error(f"Silver Initial Load Failed wit Error: {e}")
                sys.exit(1)
                
## Silver Main Ingestion Function ##
def silver_main(args):
    spark = get_spark_session()
    
    ## Spark Config for Schema Evolution ##
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    try:
        df_bronze = spark.readStream\
            .format("delta")\
            .option("ignoreChanges","true")\
            .load(args.bronze_path)
            
    except AnalysisException as e:
        log.error(f"Error While Reading from {args.bronze_path}: {e}")
        sys.exit(1)
        
    ## Silver Schema - Defined with Necessay Schema
    silver_schema = StructType([
        StructField("$schema",StringType(),True),
        StructField("meta",StructType([
            StructField("uri",StringType(),True),
            StructField("request_id",StringType(),True),
            StructField("id",StringType(),True),
            StructField("domain",StringType(),True),
            StructField("stream",StringType(),True),
        ])),
        StructField("id",StringType(),True),
        StructField("type",StringType(),True),
        StructField("namespace",StringType(),True),
        StructField("title",StringType(),True),
        StructField("title_url",StringType(),True),
        StructField("comment",StringType(),True),
        StructField("timestamp",StringType(),True),
        StructField("user",StringType(),True),
        StructField("bot",StringType(),True),
        StructField("parsedcomment",StringType(),True),
        StructField("corrupt_json",StringType(),True)
    ])
    
    ##PERMISSIVE MODE for safe Loading
    json_options = {
        "mode":"PERMISSIVE",
        "columnNameOfCorruptRecord": "corrupt_json"
    }

    ## Converting the String raw json column with Silver Schema Enforcement
    df_silver = df_bronze.withColumn(
        "value",
        from_json(col('raw_values'),
                silver_schema,json_options)
        )

    df_final = df_silver.select(
                col('value.$schema').alias("kafka_schema"),
                col("value.meta.uri").alias("meta_url"),
                col("value.meta.request_id").alias("meta_request_id"),
                col("value.meta.id").alias("meta_id"),
                col("value.meta.domain").alias("meta_domain"),
                col("value.meta.stream").alias("meta_stream"),
                col("value.id").alias("id"),
                col("value.type").alias("type"),
                col("value.namespace").alias("namespace"),
                col("value.title").alias("title"),
                col("value.title_url").alias("title_url"),
                col("value.comment").alias("comment"),
                to_timestamp(from_unixtime(col("value.timestamp"))).alias("kafka_timestamp"),
                col("value.user").alias("user"),
                col("value.bot").alias("bot"),
                col("value.parsedcomment").alias("parsedcomment"),
                col("value.corrupt_json"))\
              .withColumn(
                  "igs_timestamp",current_timestamp()
              ).withColumn(
                  "kafka_date",to_date(col('kafka_timestamp'))
              )
    
    try:
        ## Using UpsertToSilver    
        query = df_final.writeStream\
                .foreachBatch(lambda batch_df, batch_id: UpsertToSilver(batch_df,batch_id,args))\
                .option("checkpointLocation",args.checkpoint)\
                .trigger(availableNow=True)\
                .start()
        
        query.awaitTermination()
    
    except AnalysisException as e:
        log.error(f"Silver Upsert Merge Failed with Error: {e}")
        sys.exit(1)
    
    # PRODUCTION ADDITION: Optimize the table after the stream finishes

    log.info("Stream finished. Running OPTIMIZE on Silver Table...")
    try:
        deltaTable = DeltaTable.forPath(spark, args.silver_landing_layer)
        deltaTable.optimize().executeCompaction()
        log.info("Optimization complete.")
    except Exception as e:
        log.warning(f"Optimization failed (Table might not exist yet or empty): {e}")
                
if __name__ == "__main__":
    
    ## Args Parser Config ##
    parser = argparse.ArgumentParser(description="Silver Layer Ingestion Kafka")
    
    parser.add_argument("--bronze_path",required=True,type=str,help="Bronze Read Delta Path ('gs://<BUCKET_NAME>/<FOLDER_PATH>/')")
    parser.add_argument("--silver_landing_layer",required=True,type=str,help="Silver Write Delta Path ('gs://<BUCKET_NAME>/<FOLDER_PATH>/')")
    parser.add_argument("--checkpoint",required=True,type=str,help="Checkpoint Location Path ('gs://<BUCKET_NAME>/<FOLDER_PATH>/')")
    
    args = parser.parse_args()
    
    silver_main(args)
