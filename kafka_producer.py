import time
from kafka import KafkaProducer
import yfinance as yf
from datetime import date
import json

tesla = yf.Ticker("TSLA")
topic_name = 'stock_demo'

producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x:
           json.dumps(x).encode("utf-8"))

while True:
    data = tesla.history(period="1d", interval='2m')

    data = data.reset_index(drop=False)
    data['Datetime'] = data['Datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")

    # pick only the most recent stock data
    my_dict = data.iloc[-1].to_dict()

    producer.send(topic_name, key=b'Tesla Stock Update', value=my_dict)

    print(f"Producing to {topic_name}")

    producer.flush()

    time.sleep(120)