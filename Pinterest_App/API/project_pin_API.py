from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()
conf = {'bootstrap_servers':"localhost:9092"}
producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('ascii'))

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    print(type(data))
    #print(data)
    #user_encode_data = dumps(data, indent=2).encode('utf-8')
    producer.send('PinterestTopic', data)
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
