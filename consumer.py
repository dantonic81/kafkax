from confluent_kafka import Consumer
from elasticsearch import Elasticsearch, helpers
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Elasticsearch configuration
ES_HOST = os.getenv("ES_HOST")
ES_USERNAME = os.getenv("ES_ACCESS_KEY")
ES_PASSWORD = os.getenv("ES_ACCESS_SECRET")
INDEX_NAME = "tweets"
ES_URL = "https://ES_USERNAME:ES_PASSWORD@kafka-course-2325567856.eu-central-1.bonsaisearch.net:443"


es = Elasticsearch(
    ES_HOST,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    scheme="https",
    port=443,
    request_timeout=30
)

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'twitter-to-es-group',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(consumer_config)
consumer.subscribe(['twitter_tweets'])

def load_to_elasticsearch(messages):
    """
    Bulk loads messages into Elasticsearch with idempotency.
    """
    actions = []
    for message in messages:
        try:
            tweet = json.loads(message.value())
            tweet_id = tweet.get("id")  # Use "id" instead of "id_str"
            if not tweet_id:
                print(f"Skipping message without unique ID: {message.value()}")
                continue

            actions.append({
                "_index": INDEX_NAME,
                "_id": tweet_id,  # Ensure idempotency
                "_source": tweet
            })
        except json.JSONDecodeError:
            print(f"Skipping malformed message: {message.value()}")

    if actions:
        try:
            helpers.bulk(es, actions)
            print(f"Successfully indexed {len(actions)} messages into Elasticsearch.")
        except Exception as e:
            print(f"Error indexing messages: {e}")



def consume_and_index():
    """
    Consume messages from Kafka and index them into Elasticsearch.
    """
    try:
        print("Starting Kafka consumer...")
        while True:
            messages = consumer.consume(num_messages=500, timeout=5.0)

            if not messages:
                continue

            # Load messages into Elasticsearch
            load_to_elasticsearch(messages)

            # Commit offsets only after successful processing
            consumer.commit()
    except KeyboardInterrupt:
        print("Consumer interrupted. Exiting...")
    except Exception as e:
        print(f"Error in consumer: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Ensure the index exists
    if not es.indices.exists(index=INDEX_NAME):
        es.options(ignore_status=[400]).indices.create(index=INDEX_NAME)
        print(f"Created index: {INDEX_NAME}")

    # Start the consumer
    consume_and_index()
