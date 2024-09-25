import websocket,json
from time import sleep
from kafka import KafkaProducer
from json import dumps
import os
import ast

# kafka_server = ["kafka:9092"]
kafka_server = ["localhost:9092"]

producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer=lambda x: dumps(x).encode('utf-8'))

def kafka_producer(price):
    try:
        producer.send('finnhub',price)
        print(price)
        print("successful")
    except Exception as e:
        print(e)

def on_message(ws, message):
    x = json.loads(message)['data']
    kafka_producer(x[0])

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

# symbols = ast.literal_eval(os.environ["FINNHUB_STOCKS_TICKERS"])
symbols = ['AAPL']
def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')
    # for symbol in symbols:
    #     message = {
    #         'type':'subscribe',
    #         'symbol': symbol
    #     }
    #     ws.send(json.dumps(message))

if __name__ == "__main__":
    websocket.enableTrace(True)
    # ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={os.environ['FINNHUB_API_TOKEN']}",
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token=crn8tr1r01qmi0u76qqgcrn8tr1r01qmi0u76qr0",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()