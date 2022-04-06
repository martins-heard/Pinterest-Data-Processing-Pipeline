from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from kafka import KafkaConsumer
import time
import boto3
import re

def upload_to_s3(bucket_name='pinterestkafkabucket', time_stamp=None, data=None):
    s3_client = boto3.client('s3')
    #response = s3_client.upload_file(time_stamp, bucket_name, data)
    response = s3_client.put_object(Body=data, Bucket=bucket_name, Key=time_stamp)


def msg_process(msg):
    # Print the current time and the message.
    time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    #msg = str(msg)
    print(msg)
    upload_to_s3(time_stamp=f'{time_start}.json', data=msg)

running = True

def main():
    topic = 'PinterestTopic'
    app = FastAPI()
    conf = {'fetch_min_bytes':500}
    consumer = KafkaConsumer(max_poll_records=10) #topic, conf
    try:
        while running:
            consumer.subscribe(topics=[topic])
            for m in consumer:
                msg = m.value
                msg_process(msg)
            #msg = consumer #poll(5000, max_records=10)
            if m is None:
                continue

            # if msg.error():
            #     raise Exception('Error')
            else:
                msg_process(msg)
    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ =="__main__":
    main()

