import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType
)
from dotenv import load_dotenv

load_dotenv()

# AWS credentials
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')

# Snowflake credentials
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SF_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SF_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_ROLE = os.getenv('SNOWFLAKE_ROLE')

# Kafka config
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = 'stock-prices'

# Paths
S3_OUTPUT = f's3a://{S3_BUCKET}/streaming/stock_prices'
LOCAL_CHECKPOINT = os.getenv('SPARK_CHECKPOINT', 'C:/tmp/spark-checkpoints/stock-stream')

# Snowflake connection options
SNOWFLAKE_OPTIONS = {
    'sfURL': f'{SF_ACCOUNT}.snowflakecomputing.com',
    'sfUser': SF_USER,
    'sfPassword': SF_PASSWORD,
    'sfDatabase': SF_DATABASE,
    'sfWarehouse': SF_WAREHOUSE,
    'sfRole': SF_ROLE,
    'sfSchema': 'STREAMING',
}

# Schema of the JSON messages coming from Kafka
SCHEMA = StructType([
    StructField('symbol', StringType(), True),
    StructField('price', FloatType(), True),
    StructField('change_percent', StringType(), True),
    StructField('volume', IntegerType(), True),
    StructField('trading_day', StringType(), True),
    StructField('timestamp', StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka, S3, and Snowflake support."""
    return (
        SparkSession.builder
        .appName('StockPriceStreaming')
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,'
                'org.apache.hadoop:hadoop-aws:3.4.1,'
                'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
                'net.snowflake:spark-snowflake_2.13:3.0.0,'
                'net.snowflake:snowflake-jdbc:3.16.1')
        .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
        .config('spark.hadoop.fs.s3a.endpoint', f's3.{AWS_REGION}.amazonaws.com')
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.connection.timeout', '200000')
        .config('spark.hadoop.fs.s3a.attempts.maximum', '3')
        .getOrCreate()
    )


def write_batch(batch_df: DataFrame, batch_id: int):
    """
    Called by Spark for every micro-batch.
    Writes to both S3 and Snowflake.
    """
    if batch_df.isEmpty():
        print(f"⏭Batch {batch_id} — empty, skipping")
        return

    count = batch_df.count()
    print(f"\n Batch {batch_id} — {count} records")

    # Write to S3
    try:
        batch_df.write \
            .mode('append') \
            .json(S3_OUTPUT)
        print(f"S3 — wrote {count} records")
    except Exception as e:
        print(f"S3 write failed: {e}")

    # Write to Snowflake
    try:
        batch_df.write \
            .format('net.snowflake.spark.snowflake') \
            .options(**SNOWFLAKE_OPTIONS) \
            .option('dbtable', 'STOCK_TICKS') \
            .mode('append') \
            .save()
        print(f"Snowflake — wrote {count} records to STREAMING.STOCK_TICKS")
    except Exception as e:
        print(f"Snowflake write failed: {e}")


def run_consumer():
    """Main streaming consumer."""
    print("Starting Spark Structured Streaming consumer...")

    os.makedirs('C:/tmp/spark-checkpoints', exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    # Read stream from Kafka
    raw_stream = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', KAFKA_BROKER)
        .option('subscribe', TOPIC)
        .option('startingOffsets', 'earliest')
        .load()
    )

    # Parse JSON from Kafka
    parsed_stream = (
        raw_stream
        .select(
            from_json(
                col('value').cast('string'),
                SCHEMA
            ).alias('data'),
            col('partition'),
            col('offset'),
            col('timestamp').alias('kafka_timestamp')
        )
        .select(
            'data.*',
            'partition',
            'offset',
            'kafka_timestamp',
            current_timestamp().alias('processed_at')
        )
    )

    # Use foreachBatch to write to multiple sinks
    query = (
        parsed_stream.writeStream
        .foreachBatch(write_batch)
        .option('checkpointLocation', LOCAL_CHECKPOINT)
        .trigger(processingTime='30 seconds')
        .start()
    )

    print(f"Reading from Kafka topic: {TOPIC}")
    print(f"Writing to S3: {S3_OUTPUT}")
    print(f"Writing to Snowflake: STREAMING.STOCK_TICKS")
    print(f"Checkpoint: {LOCAL_CHECKPOINT}")
    print("Processing every 30 seconds...\n")

    query.awaitTermination()


if __name__ == '__main__':
    run_consumer()