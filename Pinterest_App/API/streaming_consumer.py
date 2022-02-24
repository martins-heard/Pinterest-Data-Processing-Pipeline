from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from kafka import KafkaConsumer
import time

def msg_process(msg):
    # Print the current time and the message.
    time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    val = str(msg.values())
    #dval = json.loads(val)
    print(time_start, val)

running = True

def main():
    topic = 'PinterestTopic'
    app = FastAPI()
    conf = {'bootstrap_servers':"localhost:9092"}
    consumer = KafkaConsumer() #topic, conf
    try:
        while running:
            consumer.subscribe(topics=[topic])

            msg = consumer.poll(10000, max_records=10)
            if msg is None:
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

