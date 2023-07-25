import argparse
import json
import helper
from datetime import datetime
from collections import deque
from kafka import KafkaConsumer
from s3fs import S3FileSystem


def parse_args():
    parser = argparse.ArgumentParser(
        description='Kafka Consumer')
    parser.add_argument('bucket_name', help='Your s3 bucket name')
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--topic', help='topic channel of kafka broker', default='stock_demo')
    args = parser.parse_args()
    return args

args = parse_args()

# Kafka configuration
bootstrap_server = args.bootstrap
topic = args.topic
bucket_name = args.bucket_name

consumer = KafkaConsumer(
  topic,
  bootstrap_servers=bootstrap_server,
  auto_offset_reset='latest',
  max_poll_records = 10,
  value_deserializer=lambda x: json.loads(x.decode('utf-8')))
s3 = S3FileSystem()

processed_messages = dict()

# Function to handle incoming messages
def handle_message(message):
  try:
    # Deserialize JSON message value
    print ("%s:%d:%dn: key=%sn value=%s" % (message.topic, message.partition,
                                                                     message.offset, message.key,
                                                                     message.value))
    data = message.value
    # Process the message
    stock_name = message.key.decode('utf-8')
    data_date = datetime.strptime(data['Datetime'], '%Y-%m-%d %H:%M:%S')
    
    if (not stock_name in processed_messages):
      processed_messages[stock_name] = deque()
    else:
      dq = processed_messages[stock_name]
      skip = False
      if len(dq) != 0:
        # Check for duplicate message
        if data_date == dq[-1]['Date_object']:
          print("Duplicate message detected:", data)
          skip = True
          
        # remove messages date that last more than 10 mins
        last_hr_dt = helper.get_last_hr_time()
        while len(dq) != 0 and dq[0]['Date_object'] < last_hr_dt:
          dq.popleft()

      if skip:
        return
      
      i = helper.get_running_number()
      data['stock_name'] = stock_name
      with s3.open("s3://{}/stock_market_{}.json".format(bucket_name, i), 'w') as file:
          json.dump(data, file)

      # update running_number and add new message
      helper.update_running_number(i+1)
      data['Date_object'] = data_date
      dq.append(data)
    
  except Exception as e:
    print("Error processing message:", str(e))

# Continuously poll for new messages
try:
  for message in consumer:
    handle_message(message)
except KeyboardInterrupt:
  pass
finally:
  # Close the consumer
  consumer.close()
  helper.close()