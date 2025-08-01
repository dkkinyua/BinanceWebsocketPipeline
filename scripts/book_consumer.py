import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import LongType, StringType, StructType

load_dotenv()

spark = SparkSession.builder \
                    .appName("OrderBookKafkaConsumer") \
                    .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
                    "org.postgresql:postgresql:42.7.7") \
                    .getOrCreate()

'''
Step 1: Transform data into a consistent format by reading data from topic using readStream
Step 2: Helper function, write_as_batch, that processes the data in batches, preferable for streaming
Step 3. Write batched data to db using writeStream 
'''
def transform_data():
    topic = 'order_book_data'
    schema = StructType() \
             .add("update_id", LongType()) \
             .add("symbol", StringType()) \
             .add("bestbidprice", StringType()) \
             .add("bestbidqty", StringType()) \
             .add("bestaskprice", StringType()) \
             .add("bestaskqty", StringType()) \
             .add("eventtime", LongType()) \
             .add("transactiontime", LongType())
    
    # read data from kafka topic
    df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', os.getenv("BOOTSTRAP_SERVER")) \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .option('kafka.security.protocol', 'SASL_SSL') \
            .option('kafka.sasl.mechanism', 'PLAIN') \
            .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_API_KEY")}" password="{os.getenv("KAFKA_SECRET_KEY")}";') \
            .load()

    # parse the df from json into workable format
    json_df = df.selectExpr("CAST (value AS STRING) as parsed_json") \
                .select(from_json("parsed_json", schema).alias("data")) \
                .select("data.*")

    # run transformations
    new_df = json_df.select(
        "update_id", "symbol",
        col("bestbidprice").cast("float"),
        col("bestbidqty").cast("float"),
        col("bestaskprice").cast("float"),
        col("bestaskqty").cast("float"),
        (col("eventtime") / 1000).cast("timestamp"),
        (col("transactiontime") / 1000).cast("timestamp")
    )

    return new_df

def write_as_batch(batch_df, epoch_id):
    properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    try:
        batch_df.write \
                .jdbc(url=os.getenv("DB_URL"), table="websocket.order_book_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error writing batch data: {e}")

def write_to_db():
    df = transform_data()
    path = f"/tmp/binance_streaming/book_stream_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}" # log path

    try:
        df.writeStream \
          .foreachBatch(write_as_batch) \
          .outputMode('append') \
          .option('checkpointPath', path) \
          .start() \
          .awaitTermination()
        
        print("Data written to table websocket.order_book_data successfully!")
    except Exception as e:
        print(f"Error writing to table websocket.order_book_data: {e}")

if __name__ == '__main__':
    write_to_db()