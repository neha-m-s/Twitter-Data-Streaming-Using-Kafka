import json
import time
from datetime import datetime, timedelta

# Load tweets
with open("mock_tweets.json", "r", encoding="utf-8") as file:
    tweets = json.load(file)

# Simulate sent_at timestamps (spread out by 2 seconds each)
base_time = datetime.now()
for i, tweet in enumerate(tweets):
    tweet["sent_at"] = (base_time + timedelta(seconds=i * 2)).strftime("%Y-%m-%d %H:%M:%S")

# Save updated tweets back
with open("mock_tweets.json", "w", encoding="utf-8") as file:
    json.dump(tweets, file, indent=4)

print("✅ Added sent_at to all tweets.")
