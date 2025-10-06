import tweepy
import json
from kafka import KafkaProducer
import time
import os
import random

# --- Twitter API Configuration ---
bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
if not bearer_token:
    raise ValueError("TWITTER_BEARER_TOKEN environment variable not set!")

# --- Kafka Configuration ---
KAFKA_TOPIC = 'twitter_trends'
KAFKA_SERVER = 'kafka:29092'

# --- Product Keywords for Search Query ---
product_keywords = ["cake", "tins", "pantry", "glass star", "t-light", "union jack", "hand warmer", "bird ornament", "playhouse", "mug cosy"]

def fetch_initial_tweets():
    """Connects to Twitter API v2 once to fetch a batch of recent tweets."""
    try:
        client = tweepy.Client(bearer_token)
        print("Connected to Twitter API to fetch initial tweets.")
        query = " OR ".join(product_keywords) + " -is:retweet lang:en"
        response = client.search_recent_tweets(query=query, max_results=100)
        
        if response.data:
            print(f"Successfully fetched {len(response.data)} tweets to use for the stream.")
            return [tweet.text for tweet in response.data]
        else:
            print("No tweets found. Using fallback sample data.")
            return ["Sample tweet about a nice cake tins.", "This union jack hand warmer is great!"]
            
    except Exception as e:
        print(f"Could not connect to Twitter API: {e}. Using fallback sample data.")
        return ["Sample tweet about a nice cake tins.", "This union jack hand warmer is great!"]

if __name__ == "__main__":
    print("Starting Simulated Twitter Stream Producer...")

    # 1. Fetch a batch of real tweets ONCE to use as our source material.
    tweets_buffer = fetch_initial_tweets()

    # 2. Connect to Kafka with a retry mechanism.
    producer = None
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER])
            print("Successfully connected to Kafka.")
            break
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds... ({retries - 1} retries left)")
            retries -= 1
            time.sleep(5)

    if not producer:
        print("Could not connect to Kafka after several retries. Exiting.")
        exit(1)

    print(f"Starting to send simulated stream to topic '{KAFKA_TOPIC}'...")

    # 3. Start an infinite loop to simulate a live stream using the fetched tweets.
    try:
        index = 0
        while True:
            tweet_text = tweets_buffer[index % len(tweets_buffer)]
            
            print(f"--- Sending Tweet to Kafka: {tweet_text[:50]}...")
            producer.send(KAFKA_TOPIC, value=tweet_text.encode('utf-8'))
            
            index += 1
            time.sleep(random.randint(2, 5))

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        if producer:
            producer.close()
        print("Kafka producer closed.")

