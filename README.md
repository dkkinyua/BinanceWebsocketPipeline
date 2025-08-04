# 📈 Real-Time Crypto Streaming Pipeline

This project captures real-time cryptocurrency data from Binance WebSocket API, streams it through Kafka (Confluent Cloud), processes it using PySpark, stores it in PostgreSQL, and visualizes the insights in Grafana.

## Tech Stack Used

- **Kafka (Confluent Cloud)**
- **Docker & Docker Compose**
- **PySpark Structured Streaming**
- **PostgreSQL**
- **Grafana**
- **Binance WebSocket API**

## Project Architecture

![Workflow diagram](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/syr2oyke5hwzvis00sx7.jpg)

1. **Producer**: Streams 1m Kline & Order Book data from Binance to Kafka
2. **Kafka Topics**: `kline`, `order_book_data`
3. **Consumer**: PySpark reads from Kafka and writes to PostgreSQL
4. **Database**: PostgreSQL stores structured records
5. **Dashboard**: Grafana queries PostgreSQL for real-time analytics

## 🚧 Setup

### 1. Clone Repository
```bash
git clone https://github.com/dkkinyua/BinanceWebsocketPipeline
cd BinanceWebsocketPipeline
```

### 2. Activate a virtual environment

```bash
python3 -m venv myenv
source myenv/bin/activate  # Linux/MacOS
myenv\Scripts\activate # Windows
```

### 3. Install required packages

```bash
pip install -r requirements.txt
```

### 4. Environment Variables

Create a `.env` file:

```env
BOOTSTRAP_SERVER=<your_confluent_bootstrap_server>
KAFKA_API_KEY=<your_api_key>
KAFKA_SECRET_KEY=<your_secret_key>
DB_URL=<DB_URL>
DB_USER=<YOUR_DB_USERNAME>
DB_PASSWORD=<YOUR_DB_PWD>
```

### 3. Start Docker Containers

```bash
docker-compose up --build
```
## Project Breakdown.
## Project Breakdown

### Q: Why use websockets instead of the Binance REST APIs?

When building trading bots, real-time dashboards, or streaming data pipelines from cryptocurrency exchanges like Binance, developers/engineers are often faced with a key question,

> Should I use the Binance REST API or the WebSocket API?

The differences between the two have been highlighted below:

| Feature              | REST API                                      | WebSocket API                                  |
|----------------------|-----------------------------------------------|------------------------------------------------|
| **Communication**    | Client initiates one request per data need using `requests`   | A persistent, real-time two-way connection using `websocket-client`     |
| **Latency**          | Higher latency, request → wait → response, meaning the client has to wait for some time to receive the response from the server    | Ultra-low latency as data is pushed as it changes  |
| **Efficiency**       | Inefficient for frequent updates              | Highly Efficient as there's a single connection for continuous updates |
| **Best Use Case**    | One-time data queries, low-frequency polling  | Real-time market data, price tracking, live feeds |
| **Connection Type**  | Stateless (no ongoing link)                   | Stateful (persistent connection)               |
| **Network Load**     | High if polled frequently                     | Low, since updates are sent only when needed   |
| **Data Freshness**   | Snapshot of current state only                | Stream of live updates as events occur         |

For more details on the WebSocket API, visit this [link](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info) for more details.

I have published another blog where I used the Binance REST APIs to extract, transform, and load data into a Postgres and Cassandra databases using Change Data Capture (CDC), you can access the blog [here](https://dev.to/dkkinyua/building-a-real-time-crypto-pipeline-with-binance-apis-postgresql-debezium-kafka-spark--5gbl)

### Kafka producer and consumer

Our Kafka producers and consumers are built in Python using the `confluent-kafka` package, which enables us to use the `confluent_kafka.Producer` and `confluent_kafka.Consumer` clients to connect to Confluent Cloud to produce and consume data to/and from our Kafka topics.

#### Let's produce data to a Kafka topic, **kline_data**.

- First, install the `confluent-kafka` and other necessary packages.

```bash
pip install confluent-kafka websocket-client rel
```

- Import the `confluent_kafka.Producer` to create a Producer client.

```python
from confluent_kafka import Producer
from websocket import WebSocketApp

config = {
    'bootstrap.servers': 'YOUR_BOOTSTRAP_SERVER',
    'sasl.username': 'YOUR_CONFLUENT_API_KEY',
    'sasl.password': 'YOUR_CONFLUENT_SECRET_KEY',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN'
}

producer = Producer(config)
```

- To stream data from websockets, you need to use the `websocket-client` package. Define the helper functions `on_open`, `on_close`, `on_error`, and `on_message`. The `on_message` function will be used to consume data from the websocket, and send the resulting data to the Kafka topic as shown below.
For more information on consuming data from websockets, visit the [documentation](https://pypi.org/project/websocket-client/)

```python
# helper functions
def on_open(ws):
    print("Order book websocket open for connections...")

def on_close(ws, close_status_msg, close_msg):
    print(f"Order book connection closed", close_status_msg, close_msg)

def on_error(ws, error):
    print(f"There is an error: {error}")

def on_message(ws, message):
    data = json.loads(message)
    order = data["data"]
    book_data = {
        "update_id": order["u"],
        "symbol": order["s"],
        "bestbidprice": order["b"],
        "bestbidqty": order["B"],
        "bestaskprice": order["a"],
        "bestaskqty": order["A"],
        "eventtime": order["E"],
        "transactiontime": order["T"]
    }

    try:
        producer.produce('order_book_data', json.dumps(book_data).encode('utf-8'))
        producer.poll(0)
        print("Data sent to order_book_data topic successfully!")
    except Exception as e:
        print(f"Error producing data to topic: {e}")
```

- Let's define a function `get_data()` which configures `WebSocketApp`. The **Registered Event Listener** `rel` package is a cross-platform asynchronous event dispatcher primarily designed for network applications and is used with `websocket-client` to handle events as they occur during connection.

```python
def get_data(BASE_URL, symbols):
    streams = '/'.join([f"{symbol}@bookTicker" for symbol in symbols]) # explained in the README 
    url = f'{BASE_URL}/stream?streams={streams}'

    ws = WebSocketApp(
        url,
        on_open=on_open,
        on_close=on_close,
        on_error=on_error,
        on_message=on_message
    )

    ws.run_forever(dispatcher=rel, reconnect=5)
    rel.signal(2, rel.abort)
    rel.dispatch()
```
> NOTE: Using `streams = '/'.join([f"{symbol}@bookTicker" for symbol in symbols])` is good practice as it combines all symbols into one request, hence reducing the number of requests that would be needed to get data for 10 symbols. We are using one request for all symbols as opposed to one request per symbol. Also, the WebSocket API supports multiple streams, hence making this more efficient. So the request URL to the API would look like this: `wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/dotusdt@bookTicker/avaxusdt@bookTicker...`Check the Binance [documentation](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info) for more information

- Let's use `if __name__ == '__main__'` to invoke our script whenever the script is called. `producer.flush()` is used to flush all pending messages to the topic during connection termination to prevent loss of data.

```python
if __name__ == '__main__':
    try:
        get_data(BASE_URL, symbols)
    finally:
        print("Flushing all pending messages...")
        producer.flush()
```
The data is received in our topic as shown in the snapshot below.


![Kline snapshot](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/uqrtlp2j92p7ymj3rmpd.png)


![Order Book Data snapshot](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/i736tfpgrp299pwmftcc.png)

#### Let's consume data from the Kafka topic and stream data to a Postgres database using PySpark's Structured Streaming.

- Install the necessary packages required.

```bash
pip install pyspark
```

- Import necessary modules and functions, and configure Spark for our application using `SparkSession` from `pyspark.sql`.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import BooleanType, LongType, StructType, StringType

spark = SparkSession.builder \
            .appName("SparkPostgresConsumer") \
            .master('local[*]') \
            .config('spark.ui.port', '4041') \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
                    "org.postgresql:postgresql:42.7.7") \
            .getOrCreate()
```

- Define a schema and its data types for our DataFrame. This is important to let our schema know beforehand what data and types it is dealing with.

```python
def transform_data():
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
```

- Define our DataFrame using `readStream()`, and configure Kafka settings

```python
def transform_data():
    # code
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'YOUR_CONFLUENT_SECRET_KEY') \
        .option('subscribe', 'YOUR_TOPIC') \
        .option('startingOffsets', 'earliest') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'PLAIN') \
        .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_CONFLUENT_API_KEY" password="YOUR_CONFLUENT_SECRET_KEY";') \
        .load()
```

- Our data coming from the topic is not in our ideal structure as our desired data in inside `value`. We need to extract this data and parse it into JSON format.

```python
def transform_data():
    # code 
    json_df = df.selectExpr("CAST (value AS STRING) as parsed_json") \
                .select(from_json("parsed_json", schema).alias("data")) \
                .select("data.*")
```
- Now, we can run our transformations, accessing our data from our `json_df` dataframe with the parsed JSON data. We can do so using SQL queries inside our Python code. After running transformations, return our transformed dataframe for micro-batching.

```python
def transform_data():
    # code
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
```

- Define a helper function `write_each_batch()` which writes data into the Postgres database in micro batches, ideal for streaming.

```python
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
```
- Define a function `write_to_db` which streams data into the Postgres database using the `writeStream()` and `foreachBatch(write_each_batch)` methods, and appends the data if the table exists in our database.

```python
def write_to_db():
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
```

The data is now streaming into Postgres successfully. Run a `SELECT * FROM table` query to check.


![Database snapshot](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/p4kmph49qabuq75evl5k.png)

### Containerization using Docker and Docker Compose.
**Docker** is a virtualization and open-source platform that lets you package your application and its dependencies into containers, which are lightweight, standalone units that can run anywhere, regardless of the system.

Think of a container like a "mini virtual machine", but faster and more efficient.

**Docker Compose** is a tool for defining and running multi-container Docker applications.

You need to install Docker for this project. Follow this [link](https://docs.docker.com/engine/install/) to install Docker.

For this project, different services have been defined into containers to run individually, which are:
- `kline-producer`: Producer for the 1-minute klines
- `kline-consumer`: Consumer for the 1-minute klines
- `book-producer`: Producer for the order book data
- `book-consumer`: Consumer for the order book data


- First, define a `Dockerfile` in the root of your project. A Dockerfile contains the project's instructions and what's required for the project to run smoothly in Docker.

```docker
# imports the Python image from docker hub
FROM python:3.10-slim

# installs java for pyspark to run smoothly
RUN apt-get update && apt-get install -y --no-install-recommends \
openjdk-17-jre-headless \
curl \
gnupg \
apt-transport-https \
ca-certificates && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# sets java environment vars
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# copies all contents into a folder app/
COPY . /app/

# sets app/ as the current working directory, same as cd in linux
WORKDIR /app

# installs all python packages
RUN pip install -r requirements.txt

```
- Create a folder `docker-compose.yml` file, which defines our different services.

```yaml
version: '3.8'

services:
    kline-producer:
        build: .
        command: python3 scripts/kline_producer.py
        environment:
          - KAFKA_SECRET_KEY=${KAFKA_SECRET_KEY}
          - KAFKA_API_KEY=${KAFKA_API_KEY}
          - BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER}

    kline-consumer:
        build: .
        command: python3 scripts/kline_consumer.py
        environment:
          - KAFKA_SECRET_KEY=${KAFKA_SECRET_KEY}
          - KAFKA_API_KEY=${KAFKA_API_KEY}
          - BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER}
          - DB_URL=${DB_URL}
          - DB_USER=${DB_USER}
          - DB_PASSWORD=${DB_PASSWORD}
    
    book-producer:
        build: .
        command: python3 scripts/book_producer.py
        environment:
          - KAFKA_SECRET_KEY=${KAFKA_SECRET_KEY}
          - KAFKA_API_KEY=${KAFKA_API_KEY}
          - BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER}

    book-consumer:
        build: .
        command: python3 scripts/book_consumer.py
        environment:
            - KAFKA_SECRET_KEY=${KAFKA_SECRET_KEY}
            - KAFKA_API_KEY=${KAFKA_API_KEY}
            - BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER}
            - DB_URL=${DB_URL}
            - DB_USER=${DB_USER}
            - DB_PASSWORD=${DB_PASSWORD}
```

- To boot up our containers, run the following command in your terminal

```bash
docker compose up --build
```
- If you need to stop the containers at any time, run the following command or press `Ctrl + C` on Windows or `Cmd + C` on Mac.

```bash
docker compose down
```

Below are some snapshots in Docker Desktop and the terminal


![Docker desktop snapshot](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/mzw8tgzhejshj75nkcj8.png)

![terminal snapshot](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/1gt4u0r4s0co8b7zixjd.png) 

## Contributions

All pull requests are welcome!