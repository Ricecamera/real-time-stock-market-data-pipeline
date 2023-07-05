from kafka import KafkaConsumer
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                                                      auto_offset_reset='latest',
                                                       max_poll_records = 10)
topic_name='stock_demo'
consumer.subscribe(topics=[topic_name])
consumer.subscription()
for message in consumer:
      print ("%s:%d:%dn: key=%sn value=%sn" % (message.topic, message.partition,
                                                                     message.offset, message.key,
                                                                     message.value))