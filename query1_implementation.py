import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["eurovision"]
collection = db["tweets"]

TARGET_USER = "blcklcfr"

# Fetch all tweets by the target user
user_tweets = list(collection.find({"user.screen_name": TARGET_USER}))
print(f"Found {len(user_tweets)} tweets by {TARGET_USER}")

# Build a lookup map by tweet _id for fast access
tweet_map = {t["_id"]: t for t in user_tweets}

# Identify root tweets (not a reply, or reply to someone outside their own chain)
threads = []
visited = set()

for tweet in user_tweets:
    if tweet.get("in_reply_to_status_id_str") is None:
        # This is a root tweet, start a thread
        thread = []
        current = tweet

        while current is not None:
            thread.append(current)
            visited.add(current["_id"])

            # Find the next reply in the chain by this user
            next_tweet = collection.find_one({
                "in_reply_to_status_id_str": current["_id"],
                "user.screen_name": TARGET_USER
            })
            current = next_tweet

        threads.append(thread)

# Sort each thread by created_at
for thread in threads:
    thread.sort(key=lambda t: t.get("timestamp_ms", "0"))

# Print results
print("\n" + "="*60)
for i, thread in enumerate(threads):
    print(f"\nTHREAD {i+1} ({len(thread)} tweets):")
    print("-" * 60)
    for tweet in thread:
        print(f"  Text:       {tweet.get('text')}")
        print(f"  Time:       {tweet.get('created_at')}")
        print(f"  ID:         {tweet.get('_id')}")
        print(f"  Reply to:   {tweet.get('in_reply_to_status_id_str')}")
        print(f"  Name:       {tweet['user'].get('name')}")
        print(f"  Screen name:{tweet['user'].get('screen_name')}")
        print()
