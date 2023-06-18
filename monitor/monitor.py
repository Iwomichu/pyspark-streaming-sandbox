import logging
from datetime import datetime
from logging import getLogger
from time import sleep

from fastkafka import FastKafka
from pydantic import BaseModel

sleep(10)
logger = getLogger("Demo Kafka app")
posts_logger = getLogger("POST")
trends_logger = getLogger("SENTIMENTS")
logging.basicConfig(level=logging.INFO)


class Post(BaseModel):
    body: str
    id: int
    timestamp: datetime
    poster_id: int


class SentimentItem(BaseModel):
    id: int
    sentiment: str


class SentimentMessage(BaseModel):
    key: int
    value: SentimentItem


kafka_brokers = {
    "localhost": {
        "url": "localhost",
        "description": "local development kafka broker",
        "port": 9092,
    },
    "compose": {
        "url": "kafka",
        "description": "local development kafka broker",
        "port": 9092,
    },
}

kafka_app = FastKafka(
    title="Demo Kafka app",
    kafka_brokers=kafka_brokers,
)


@kafka_app.consumes(topic="posts")
async def on_post(msg: Post):
    posts_logger.info(msg)


@kafka_app.consumes(topic="sentiments")
async def on_sentiment(msg: SentimentItem):
    trends_logger.info(msg)
