from kafka import KafkaConsumer
from s3fs import S3FileSystem

from collections import deque
import json
import helper

# Kafka configuration
bootstrap_server = 'localhost:9092'
topic = 'stock_demo'

consumer = KafkaConsumer(
  topic,
  bootstrap_servers=bootstrap_server,
  auto_offset_reset='latest',
  max_poll_records = 10,
  value_deserializer=lambda x: json.loads(x.decode('utf-8')))
s3 = S3FileSystem()

processed_messages = deque()

# Function to handle incoming messages
def handle_message(message):
  try:
    # Deserialize JSON message value
    print ("%s:%d:%dn: key=%sn value=%s" % (message.topic, message.partition,
                                                                     message.offset, message.key,
                                                                     message.value))
    data = message.value
    
    # remove messages date that last more than 10 mins
    last_10_dt = helper.get_last_10m_time()
    while processed_messages[0] < last_10_dt:
      processed_messages.popleft()

    # # Check for duplicate message
    if data['Datetime'] == processed_messages[-1]['Datetime']:
        print("Duplicate message detected:", data)
        return

    # Process the message
    data['stock_name'] = message.key
    i = helper.get_running_number()
    with s3.open("s3://socket-market-data-sahatsarin/stock_market_{}.json".format(i), 'w') as file:
        json.dump(data, file)

    # update running_number and add new message
    helper.update_running_number(i+1)
    processed_messages.append(data['Datetime'])
    
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