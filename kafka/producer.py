import os
import json
import time
import logging
from kafka import KafkaProducer
from datetime import datetime

#Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Set SSL certificate 
os.environ['SSL_CERT_FILE'] = r"C:\Users\NEHA M S\cacert.pem" 

#Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Kafka Producer initialized.")
except Exception as e:
    logging.error(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

# Send mock tweets to Kafka
def stream_mock_tweets():
    try:
        with open("mock_tweets.json", "r", encoding="utf-8") as file:
            tweets = json.load(file)
            for tweet in tweets:
                tweet_data = {
                    "tweet_id": tweet.get("id"),
                    "tweet_text": tweet.get("text"),
                    "created_at": tweet.get("created_at"),
                    "user_name": tweet.get("user", "anonymous"),  
                    "hashtags": [word for word in tweet["text"].split() if word.startswith("#")],
                    "sent_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  
                }
                logging.info(f"Sending tweet: {tweet_data['tweet_text']}")
                producer.send('tweets_topic', tweet_data)
                time.sleep(2)  
    except FileNotFoundError:
        logging.error("mock_tweets.json file not found. Please create it in the same directory.")
    except Exception as e:
        logging.error(f"Error while reading or sending tweets: {e}")

if __name__ == "__main__":
    stream_mock_tweets()
