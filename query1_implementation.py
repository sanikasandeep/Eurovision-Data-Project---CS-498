import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["eurovision"]
collection = db["tweets"]

TARGET_USER = "blcklcfr"

# We will use the index on user.screen_name to quickly filter tweets, and the "in_reply_to_status_id_str" index to traverse reply chains. 
# Then, we build threads by following parent-child relationships, and finally sort by time created

# fetch all tweets by the target user using index
user_tweets = list(collection.find({"user.screen_name": TARGET_USER}))
print(f"Found {len(user_tweets)} tweets by {TARGET_USER}")

# make lookup map by tweet _id 
tweet_map = {t["_id"]: t for t in user_tweets}

# identify root tweets (non reply-tweets)
threads = []
visited = set()

for tweet in user_tweets:
    if tweet.get("in_reply_to_status_id_str") is None:
        # make new tweet
        thread = []
        current = tweet

        # traverse through replies
        while current is not None:
            thread.append(current)
            visited.add(current["_id"])

            # use index on in_reply_to_status_id_str to find next reply
            next_tweet = collection.find_one({
                "in_reply_to_status_id_str": current["_id"],
                "user.screen_name": TARGET_USER
            })
            current = next_tweet

        threads.append(thread)

# sort each thread by timestamp
for thread in threads:
    thread.sort(key=lambda t: t.get("timestamp_ms", "0"))

# output results
print()
print("="*60)
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
