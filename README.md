# Realtime Stock data pipeline
<div align="center">
<img src="system diagram.png"/>
</div>

This application extracts stock data from yahoo finance api using [yfinace](https://pypi.org/project/yfinance/) python libraries and ingests it into Amazon S3 bucket with [apache kafka](https://kafka.apache.org/). This application is built to work in realtime and can handle data with time duplication.

## Pre requisites
- Python 3.6+
- Docker
- Amazon CLI and S3 bucket

## Run the program
1. setup enviroments
```cli
docker compose up
pip install -r requirements.txt
```

2. run producer and consumer application
```
python kafka_producer.py ${TICKER} \
    ${TIME_INTERVAL (minute)} \
    [--bootstrap ${BROKER_URL}] \
    [--topic ${TOPIC_NAME}]
python kafka_conusmer.py ${BUCKET_NAME} \
    [--bootstrap ${BROKER_URL}] \
    [--topic ${TOPIC_NAME}]
```
## Reference
- [Tutorial by Analytics Vidhya](https://www.analyticsvidhya.com/blog/2022/09/build-a-simple-realtime-data-pipeline/)
- [Tutorial Video by Darshil Parmar](https://www.youtube.com/watch?v=KerNf0NANMo&t=832s)
