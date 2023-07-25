import time
import json
import argparse
import schedule
import yfinance as yf
from kafka import KafkaProducer
from helper import is_market_open

def parse_args():
    parser = argparse.ArgumentParser(
        description='Kafka Producer')
    parser.add_argument('ticker', help='stock name to extract data from')
    parser.add_argument('min', type=int, help='stock data interval in minute')
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--topic', help='topic channel of kafka broker', default='stock_demo')

    args = parser.parse_args()
    return args

args = parse_args()

# Kafka configuration
bootstrap_server = args.bootstrap
topic = args.topic
ticker_symbol = args.ticker
interval_min = args.min

# Create Kafka producer
producer = KafkaProducer(
  bootstrap_servers=[bootstrap_server],
  value_serializer=lambda x:
  json.dumps(x).encode("utf-8"))

def extract_stock_data(ticker, interval):
  if is_market_open():
    stock = yf.Ticker(ticker)
    data = stock.history(period='1d', interval=interval)

    data = data.reset_index(drop=False)
    # Convert timestamp to string
    data['Datetime'] = data['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    my_dict = data.iloc[-1].to_dict()
    # Send data to Kafka
    producer.send(topic, key=bytes(ticker, 'utf-8'), value=my_dict)
    producer.flush()
    print(f"Producing to {topic}")
  else:
    print("Market is closed. No data available.")

def run_scheduler(ticker, mins=1):
# schedule script to run every 1 minutes
  interval = str(mins) + 'm'
  schedule.every(mins).minutes.do(extract_stock_data, ticker=ticker, interval=interval)
  while True:
    schedule.run_pending()
    time.sleep(5) # sleep for 5 seconds

run_scheduler(ticker_symbol, interval_min)