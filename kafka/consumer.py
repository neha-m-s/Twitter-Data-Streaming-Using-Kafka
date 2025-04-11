import json
import os
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
import logging
import signal
import sys
from datetime import datetime

# Load .env variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL config
DB_NAME = os.getenv("DB_NAME", "twitter_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "yourpassword")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    logging.info("✅ Connected to PostgreSQL")
except Exception as e:
    logging.error(f"❌ Failed to connect to PostgreSQL: {e}")
    sys.exit(1)

# Setup Kafka consumer
try:
    consumer = KafkaConsumer(
        'tweets_topic',
        bootstrap_servers='localhost:9092',
        group_id='tweet_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    logging.info("✅ Kafka consumer connected and subscribed to 'tweets_topic'")
except Exception as e:
    logging.error(f"❌ Failed to connect to Kafka: {e}")
    sys.exit(1)

# Graceful shutdown
def shutdown_handler(sig, frame):
    logging.info("🛑 Shutting down gracefully...")
    cur.close()
    conn.close()
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# Listen for tweets and insert
logging.info("🔁 Listening for tweets...")
for message in consumer:
    tweet = message.value
    processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sent_at = tweet.get("sent_at")

    logging.info(f"📥 Received tweet at {processing_time}: ID {tweet.get('tweet_id')}")

    try:
        cur.execute("""
            INSERT INTO tweets (tweet_id, user_name, tweet_text, created_at, hashtags, sent_at, processing_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tweet_id) DO NOTHING;
        """, (
            tweet.get("tweet_id"),
            tweet.get("user_name", "anonymous"),
            tweet.get("tweet_text"),
            tweet.get("created_at"),
            tweet.get("hashtags"),
            tweet.get("sent_at"),
            processing_time
        ))
        conn.commit()
        logging.info(f"✅ Stored tweet ID: {tweet.get('tweet_id')}")
    except Exception as e:
        logging.error(f"❌ Error inserting tweet ID {tweet.get('tweet_id')}: {e}")
        conn.rollback()
