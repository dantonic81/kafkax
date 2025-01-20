import tweepy
from dotenv import load_dotenv
from confluent_kafka import Producer
import json
import os
import time


load_dotenv()

# Twitter API credentials
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_KEY_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")
# Use Bearer Token for Streaming API
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

# Kafka configuration
KAFKA_TOPIC = "twitter_tweets"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

# Kafka producer configuration with idempotence enabled
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
    'enable.idempotence': True,  # Enable idempotence
    'acks': 'all',              # Ensure full acknowledgment
    'retries': 5,               # Configure retries (default is 2, but can increase)
    'max.in.flight.requests.per.connection': 5,  # Prevent reordering
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch_tweets():
    client = tweepy.Client(bearer_token=BEARER_TOKEN)

    query = "Python -is:retweet"  # Adjust query as needed
    try:
        tweets = client.search_recent_tweets(query=query, tweet_fields=["id", "text", "created_at"], max_results=10)
        for tweet in tweets.data:
            try:
                # Prepare tweet data
                tweet_data = {
                    "id": tweet.id,
                    "text": tweet.text,
                    "created_at": tweet.created_at.isoformat()  # Convert datetime to string
                }

                # Send tweet data to Kafka
                producer.produce(
                    KAFKA_TOPIC,
                    key=str(tweet.id),
                    value=json.dumps(tweet_data),
                    callback=delivery_report
                )
                producer.flush()
            except Exception as e:
                print(f"Error sending tweet to Kafka: {e}")
        # Add delay to avoid rate limits
        time.sleep(15)  # Adjust this delay as needed
    except tweepy.errors.TooManyRequests as e:
        print("Rate limit reached. Waiting for reset...")
        reset_time = int(e.response.headers.get("x-rate-limit-reset", time.time() + 60))
        wait_time = reset_time - int(time.time())
        time.sleep(wait_time)
        fetch_tweets()  # Retry after the wait



if __name__ == "__main__":
    fetch_tweets()
