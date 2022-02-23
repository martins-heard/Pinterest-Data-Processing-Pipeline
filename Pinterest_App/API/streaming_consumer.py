from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from kafka import KafkaConsumer
import time

# class Data(BaseModel):
#     category: str
#     index: int
#     unique_id: str
#     title: str
#     description: str
#     follower_count: str
#     tag_list: str
#     is_image_or_video: str
#     image_src: str
#     downloaded: int
#     save_location: str

def msg_process(msg):
    # Print the current time and the message.
    # time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    val = str(msg.values())
    #dval = json.loads(val)
    print(val)

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

