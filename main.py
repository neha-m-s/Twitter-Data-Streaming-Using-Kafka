import os
from dotenv import load_dotenv
from kafka import KafkaProducer
import json
import requests
import time

load_dotenv()

BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tweets_topic")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Twitter API headers
headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

# Define search endpoint and parameters
search_url = "https://api.twitter.com/2/tweets/search/recent"
query_params = {
    'query': 'data science',
    'max_results': 10,
    'tweet.fields': 'id,text,author_id,created_at'
}

def get_tweets():
    response = requests.get(search_url, headers=headers, params=query_params)
    if response.status_code != 200:
        raise Exception(f"Request returned error: {response.status_code}, {response.text}")
    return response.json().get("data", [])

def send_to_kafka(tweets):
    for tweet in tweets:
        print(f"Sending tweet: {tweet['text']}")
        producer.send(KAFKA_TOPIC, tweet)
        time.sleep(1)  # Throttle to avoid rate limits

if __name__ == "__main__":
    print("Fetching tweets and sending to Kafka...")
    tweets = get_tweets()
    send_to_kafka(tweets)
    print("All tweets sent!")
