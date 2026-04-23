import json
import pymongo
from pathlib import Path

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["eurovision"]
collection = db["tweets"]

def prune_tweet(tweet):
    id_str = tweet.get("id_str")
    if not id_str:
        return None
    user = tweet.get("user", {})
    pruned_user = {
        "id_str": user.get("id_str"),
        "name": user.get("name"),
        "screen_name": user.get("screen_name"),
        "verified": user.get("verified", False)
    }
    entities = tweet.get("entities", {})
    hashtags = [h["text"].lower() for h in entities.get("hashtags", []) if "text" in h]
    mentions = [
        {"id_str": m.get("id_str"), "screen_name": m.get("screen_name")}
        for m in entities.get("user_mentions", [])
    ]
    place = tweet.get("place")
    pruned_place = None
    if place:
        pruned_place = {
            "country": place.get("country"),
            "country_code": place.get("country_code")
        }
    pruned = {
        "_id": id_str,
        "created_at": tweet.get("created_at"),
        "timestamp_ms": tweet.get("timestamp_ms"),
        "text": tweet.get("text"),
        "user": pruned_user,
        "place": pruned_place,
        "in_reply_to_status_id_str": tweet.get("in_reply_to_status_id_str"),
        "in_reply_to_user_id_str": tweet.get("in_reply_to_user_id_str"),
        "is_retweet": "retweeted_status" in tweet,
        "is_quote_status": tweet.get("is_quote_status", False),
        "entities": {
            "hashtags": hashtags,
            "user_mentions": mentions
        }
    }
    return pruned

def load_file(filepath):
    batch = []
    inserted = 0
    duplicates = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                tweet = json.loads(line)
                pruned = prune_tweet(tweet)
                if pruned is None:
                    continue
                batch.append(pruned)
                if len(batch) >= 500:
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        inserted += len(result.inserted_ids)
                    except pymongo.errors.BulkWriteError as e:
                        inserted += e.details["nInserted"]
                        duplicates += len(e.details["writeErrors"])
                    batch = []
            except json.JSONDecodeError:
                continue
    if batch:
        try:
            result = collection.insert_many(batch, ordered=False)
            inserted += len(result.inserted_ids)
        except pymongo.errors.BulkWriteError as e:
            inserted += e.details["nInserted"]
            duplicates += len(e.details["writeErrors"])
    print(f"{filepath}: {inserted} inserted, {duplicates} duplicates skipped")

for i in range(3, 11):
    path = Path(f"Eurovision{i}.json")
    if path.exists():
        print(f"Loading {path}...")
        load_file(path)

print("Creating indexes...")
collection.create_index([("user.screen_name", 1)])
collection.create_index([("user.id_str", 1)])
collection.create_index([("created_at", 1)])
collection.create_index([("place.country", 1)])
collection.create_index([("entities.hashtags", 1)])
collection.create_index([("in_reply_to_status_id_str", 1)])
collection.create_index([("user.verified", 1)])
print("Done!")
