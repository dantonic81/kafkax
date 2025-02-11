import tweepy
from dotenv import load_dotenv
from confluent_kafka import Producer
import json
import os
import time

load_dotenv()

# Twitter API credentials
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

# Kafka configuration
KAFKA_TOPIC = "twitter_tweets"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
    'enable.idempotence': True,
    'acks': 'all',
    'retries': 5,
    'max.in.flight.requests.per.connection': 5,
    'batch.num.messages': 1000,
    'linger.ms': 20,
    'compression.type': 'snappy',
})


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def fetch_tweets(max_requests=10):
    client = tweepy.Client(bearer_token=BEARER_TOKEN)
    query = "Python -is:retweet"

    # Limit the number of requests per run
    requests_sent = 0

    while requests_sent < max_requests:
        try:
            tweets = client.search_recent_tweets(query=query, tweet_fields=["id", "text", "created_at"],
                                                 max_results=10)  # Fetch 5 tweets per request
            if tweets.data:
                for tweet in tweets.data:
                    tweet_data = {
                        "id": tweet.id,
                        "text": tweet.text,
                        "created_at": tweet.created_at.isoformat(),
                    }
                    producer.produce(KAFKA_TOPIC, key=str(tweet.id), value=json.dumps(tweet_data),
                                     callback=delivery_report)

            producer.flush()
            requests_sent += 1
            print(f"Request {requests_sent}/{max_requests} completed.")
            time.sleep(30)  # Increase sleep time between requests

        except tweepy.errors.TooManyRequests as e:
            reset_time = int(e.response.headers.get("x-rate-limit-reset", time.time() + 60))
            wait_time = reset_time - int(time.time())
            print(f"Rate limit reached. Waiting for {wait_time} seconds before retrying.")
            time.sleep(wait_time)  # Wait until the rate limit resets
            fetch_tweets(max_requests - requests_sent)  # Retry the remaining requests


if __name__ == "__main__":
    fetch_tweets(max_requests=10)  # Limit to 10 requests
