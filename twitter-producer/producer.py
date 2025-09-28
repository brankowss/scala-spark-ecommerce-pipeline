# twitter-producer/producer.py

# Import necessary libraries
import tweepy  # The official Python library for accessing the Twitter API v2
import json  # Used for parsing API responses
from kafka import KafkaProducer  # The client for sending messages to Kafka topics
import time  # Used to add delays to the script
import os  # Used to access environment variables for security

# --- Twitter API Configuration ---
# Securely retrieve the Bearer Token from an environment variable.
bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
if not bearer_token:
    raise ValueError("TWITTER_BEARER_TOKEN environment variable not set!")

# --- Kafka Configuration ---
# Define the name of the Kafka topic where tweets will be sent.
KAFKA_TOPIC = 'twitter_trends'
# Define the address of the Kafka broker. 'kafka:29092' is the internal Docker network address.
KAFKA_SERVER = 'kafka:29092'

# --- Product Keywords for Search Query ---
# This list contains the keywords that will be used to build a search query.
product_keywords = ["cake", "tins", "pantry", "glass star", "t-light", "union jack", "hand warmer", "bird ornament", "playhouse", "mug cosy"]

# This block runs when the script is executed directly.
if __name__ == "__main__":
    print("Starting Twitter Historical Search Producer...")
    
    # Initialize the main Tweepy client with your Bearer Token.
    # This client is used for most v2 API endpoints, including search.
    try:
        client = tweepy.Client(bearer_token)
        print("Connected to Twitter API.")
    except Exception as e:
        print(f"Error connecting to Twitter API: {e}")
        exit()

    # Initialize the Kafka producer, connecting to the Kafka server.
    producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER])
    print("Connected to Kafka.")

    # Build the search query string for the Twitter API.
    # " OR " means we want tweets containing ANY of the keywords.
    # "-is:retweet lang:en" excludes retweets and ensures English language.
    query = " OR ".join(product_keywords) + " -is:retweet lang:en"
    
    print(f"Searching for the last 100 tweets with query: {query}")

    try:
        # This is the main API call. It uses the search_recent_tweets method to
        # get historical tweets from the last 7 days.
        # max_results is set to 100 to be safe with API limits.
        response = client.search_recent_tweets(query=query, max_results=100)

        # The response object can be empty if no tweets are found.
        if response.data:
            print(f"Found {len(response.data)} tweets. Sending to Kafka...")
            # Loop through each tweet found in the response.
            for tweet in response.data:
                tweet_text = tweet.text
                print(f"--- Sending Tweet to Kafka: {tweet_text[:50]}...")
                # Send each tweet's text to our Kafka topic.
                producer.send(KAFKA_TOPIC, value=tweet_text.encode('utf-8'))
                # Add a 60-second pause to be extra safe with API limits.
                time.sleep(60)
        else:
            print("No tweets found for the given keywords in the last 7 days.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # This ensures the producer connection is closed cleanly when the script finishes.
        producer.close()
        print("Finished sending tweets. Kafka producer closed.")