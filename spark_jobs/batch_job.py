from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_timestamp

# === Paths ===
json_path = "C:/Users/NEHA M S/DBT_Project/mock_tweets.json"
postgres_jdbc_url = "jdbc:postgresql://localhost:5432/twitter_data"
jdbc_jar = "C:/Users/NEHA M S/DBT_Project/docker/jars/postgresql-42.7.3.jar"

# === Create Spark Session ===
spark = SparkSession.builder \
    .appName("TweetBatchJob") \
    .config("spark.jars", jdbc_jar) \
    .getOrCreate()

# === Read Tweets from JSON File ===
df = spark.read.json(json_path)

# === Convert sent_at to Timestamp Type ===
df = df.withColumn("sent_at", to_timestamp(col("sent_at"), "yyyy-MM-dd HH:mm:ss"))

# === Add Processing Time Column ===
df = df.withColumn("processing_time", current_timestamp())

# === Write to PostgreSQL (batch_tweets table) ===
df.write \
    .format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("dbtable", "batch_tweets") \
    .option("user", "postgres") \
    .option("password", "yourpassword") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Batch job completed: Data written to batch_tweets.")

# === Stop Spark ===
spark.stop()
