import os
import rel
import json
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

# websocket helper funcs
def on_open(ws):
        print("Kline Websocket Connection open to receive connections...")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed", close_status_code, close_msg)

def on_error(ws, error):
    print(f"Error: {error}")

def on_message(ws, message):
    data = json.loads(message)
    kline = data['data']['k']

    kline_data = {
        'symbol': kline['s'],
        'interval': kline["i"],
        'open': kline['o'],
        'high': kline['h'],
        'close': kline['c'],
        'low': kline['l'],
        'klineClosed': kline['x'],
        'volume': kline['q'],
        'numTrades': kline['n'],
        'startTime': kline['t'],
        'closeTime': kline['T']
    }
    try:
        producer.produce("kline_data", json.dumps(kline_data).encode("utf-8"))
        print("Data sent successfully")
    except Exception as e:
         print(f"Error sending data to topic: {e}")

def get_data(BASE_URL, symbols):
        streams = "/".join([f"{symbol}@kline_1m" for symbol in symbols])
        url = f"{BASE_URL}/stream?streams={streams}"

        ws = WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error
        )

        ws.run_forever(dispatcher=rel, reconnect=5)
        rel.signal(2, rel.abort)  
        rel.dispatch()

if __name__ == '__main__':
    get_data(BASE_URL, symbols)