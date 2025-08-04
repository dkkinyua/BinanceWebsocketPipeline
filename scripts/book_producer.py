import os
import json
import rel
from confluent_kafka import Producer
from dotenv import load_dotenv
from websocket import WebSocketApp

load_dotenv()

config = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVER"),
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_SECRET_KEY"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN'
}

BASE_URL = "wss://fstream.binance.com"
symbols = [
    "btcusdt",   
    "ethusdt",
    "bnbusdt",
    "xrpusdt",
    "solusdt",
    "adausdt",
    "dogeusdt",
    "maticusdt",
    "dotusdt",
    "avaxusdt"
]

producer = Producer(config)

# websocket functions
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

if __name__ == '__main__':
    try:
        get_data(BASE_URL, symbols)
    finally:
        print("Flushing all pending messages...")
        producer.flush()