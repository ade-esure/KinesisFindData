#Imports & Config
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, TimestampType, LongType
 
bronze_schema = (
    StructType()
    .add("dataset_id", StringType())
    .add("event_id", StringType())
    .add("environment_id", StringType())
    .add(
        "payload",
        StructType()
        .add(
            "header",
            StructType()
            .add("brand_code", StringType())
            .add("domain", StringType())
            .add("event_origin", StringType())
            .add("event_timestamp", TimestampType())
            .add("event_type", StringType())
            .add("source_id", StringType())
            .add("source_version", StringType()),
        )
        .add("data", StringType())
        .add("payload_version", StringType())
        .add("status", StructType().add("statusCode", LongType()).add("statusMessage", StringType())),
    )
)
 
stream = "awie-kds-test-eventservice-customer-01"
region = "eu-west-1"
role = "arn:aws:iam::375857942905:role/ec-iamr-test-np-datalake-crossaccount-access-01"
initial_position = "TRIM_HORIZON"
 
kinesis = (
    spark.readStream.format("kinesis").option("streamName", stream).option("region", region).option("roleArn", role).option("initialPosition", initial_position).load()
)
 
datasets = ["ses_email_input_fields"]



#Streaming Query
stream_query = (
    kinesis.selectExpr("CAST(data as STRING) jsonData")
    .select(F.from_json("jsonData", bronze_schema).alias("events"))
    .filter(F.col("events.dataset_id").isin(datasets))
    .withColumn("partition_date", F.to_date(F.col("events.payload.header.event_timestamp")))
    .select("events.*", "partition_date")
    .writeStream.trigger(processingTime="5 minutes")
    .queryName("test1_bronze")
#     .partitionBy("dataset_id", "partition_date")
    .format("memory")
    .outputMode("append")
    .start()
#     .option("checkpointLocation", "/delta/events/_checkpoints/test1/customer/bronze2")
#     .start("s3a://awie-s3-ec-np-datalake-int-test1-01/streaming_events/customer/data/bronze2")
)

#Table
bronze_table = spark.table("test1_bronze")
bronze_table.count()

#Analyse - make some change
import pyspark.sql.functions as F
display(bronze_table.select("dataset_id").groupBy("dataset_id").count())

display(bronze_table)
#Learning some issues

