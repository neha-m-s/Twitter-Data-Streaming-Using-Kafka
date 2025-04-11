from flask import Flask, render_template, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# ✅ Connect to your Dockerized PostgreSQL
conn = psycopg2.connect(
    host="localhost",         # Same for Docker + host OS
    database="twitter_data",
    user="postgres",
    password="yourpassword"
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/tweets')
def get_tweets():
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT tweet_id, tweet_text, created_at, user_name FROM tweets ORDER BY created_at DESC LIMIT 20;")
    tweets = cur.fetchall()
    cur.close()
    return jsonify(tweets)

if __name__ == '__main__':
    app.run(debug=True)
