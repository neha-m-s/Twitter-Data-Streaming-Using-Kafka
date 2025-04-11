-- Create main table for storing tweets from streaming pipeline
CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT PRIMARY KEY,
    tweet_text TEXT,
    created_at TIMESTAMP,
    user_id TEXT,
    hashtags TEXT,
    sent_at TIMESTAMP,          -- Optional: from producer
    processing_time TIMESTAMP   -- Optional: from consumer/Spark
);

-- Optionally, create a separate table for batch results
-- This helps with comparing streaming vs batch mode
CREATE TABLE IF NOT EXISTS batch_tweets (
    tweet_id TEXT PRIMARY KEY,
    tweet_text TEXT,
    created_at TIMESTAMP,
    user_id TEXT,
    hashtags TEXT,
    sent_at TIMESTAMP,
    processing_time TIMESTAMP
);

-- View for comparing the counts (example usage)
CREATE OR REPLACE VIEW hashtag_counts AS
SELECT 
    unnest(string_to_array(hashtags, ',')) AS hashtag,
    COUNT(*) AS count
FROM tweets
GROUP BY hashtag
ORDER BY count DESC;
