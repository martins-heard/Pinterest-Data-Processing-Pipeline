from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from kafka import KafkaConsumer
import time
import boto3
import re

class b_consumer:
    """
    This is a class to send data from pinterest (user emulator) from a kafka consumer to Amazon Web Services (AWS) S3 storage.
    Each event is saved as a separate json file.
    Run this after the user_emulator.py and project_pin_API.py files are running to start storing the batch data.

    Parameters
    ----------
    s3_bucket : str
        Name of the AWS S3 bucket you wish to send the pinterest user data to. 
    """
    def __init__(self, s3_bucket: str = 'pinterestkafkabucket'):
        self.s3_bucket = s3_bucket
        self.running = True
        self.i = 0

    def _upload_to_s3(self, time_stamp=None, data=None):
        """
        This is a private method to send the processed event to S3 storage.
        """
        s3_client = boto3.client('s3')
        #response = s3_client.upload_file(time_stamp, bucket_name, data)
        response = s3_client.put_object(Body=data, Bucket=self.s3_bucket, Key=time_stamp)

    def _msg_process(self, msg):
        """
        This is a private method to process the events as a json file
        """
        # Print the current time and the message.
        time_start = time.strftime("%Y-%m-%d %H:%M:%S")
        #msg = str(msg)
        print(msg)
        self._upload_to_s3(time_stamp=f'{time_start}.json', data=msg)

    def main(self):
        """
        This is a method to read data into the kafka consumer and process the events
        """
        topic = 'PinterestTopic'
        app = FastAPI()
        conf = {'fetch_min_bytes':500}
        consumer = KafkaConsumer(max_poll_records=10) #topic, conf
        try:
            while self.running:
                consumer.subscribe(topics=[topic])
                for m in consumer:
                    self.i += 1
                    if self.i == 11:
                        consumer.close()
                        print("10 items consumed. Consuming stopped.")
                        return
                    msg = m.value
                    self._msg_process(msg)
                #msg = consumer #poll(5000, max_records=10)
                if m is None:
                    continue

                # if msg.error():
                #     raise Exception('Error')
                else:
                    self.msg_process(msg)
        except KeyboardInterrupt:
            pass

        finally:
            consumer.close()

if __name__ =="__main__":
    consume = b_consumer()
    consume.main()

