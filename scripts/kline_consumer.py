import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import BooleanType, LongType, StructType, StringType

load_dotenv()

spark = SparkSession.builder \
            .appName("SparkPostgresConsumer") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
                    "org.postgresql:postgresql:42.7.7") \
            .getOrCreate()

def transform_data():
    topic = 'kline_data'

    schema = StructType() \
        .add("close", StringType()) \
        .add("open", StringType()) \
        .add("high", StringType()) \
        .add("low", StringType()) \
        .add("interval", StringType()) \
        .add("klineClosed", BooleanType()) \
        .add("symbol", StringType()) \
        .add("volume", StringType()) \
        .add("numTrades", LongType()) \
        .add("closeTime", LongType()) \
        .add("startTime", LongType())
    
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', os.getenv("BOOTSTRAP_SERVER")) \
        .option('subscribe', topic) \
        .option('startingOffsets', 'earliest') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'PLAIN') \
        .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_API_KEY")}" password="{os.getenv("KAFKA_SECRET_KEY")}";') \
        .load()
    
    json_df = df.selectExpr("CAST (value AS STRING) as parsed_json") \
                .select(from_json("parsed_json", schema).alias("data")) \
                .select("data.*")

    new_df = json_df.select(
        col("close").cast("float"), 
        col("high").cast("float"), 
        col("low").cast("float"), 
        col("open").cast("float"), 
        col("volume").cast("float"), 
        "symbol", "interval", 
        col("numTrades").alias("num_trades"), 
        col("klineClosed").alias("isKlineClosed"),
        (col("closeTime") / 1000).cast("timestamp").alias("closetime"),
        (col("startTime") / 1000).cast("timestamp").alias("starttime")
    )
    
    return new_df

def write_each_batch(batch_df, epoch_id):
    properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    try:
        batch_df.write \
                .jdbc(url=os.getenv("DB_URL"), table="websocket.kline_1m_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error writing batch data: {e}")
    
def load():
    df = transform_data()
    path = f"/tmp/binance_streaming/kline_stream_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    try:
        df.writeStream \
        .foreachBatch(write_each_batch) \
        .outputMode("append") \
        .option('checkpointLocation', path) \
        .start() \
        .awaitTermination()

        print("Data loaded into Postgres successfully!")
    except Exception as e:
        print(f"Error loading data to db: {e}")

if __name__ == '__main__':
    load()