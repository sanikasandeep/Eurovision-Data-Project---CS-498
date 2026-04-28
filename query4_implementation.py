import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["eurovision"]
collection = db["tweets"]

pipeline = [
    # Unwind hashtags array into individual records
    {"$unwind": "$entities.hashtags"},
    # Group by hashtag and count
    {"$group": {"_id": "$entities.hashtags", "tweet_count": {"$sum": 1}}},
    # Sort by descending count
    {"$sort": {"tweet_count": -1}},
    # Top 100
    {"$limit": 100}
]

results = list(collection.aggregate(pipeline))
print(f"Top 100 Hashtags:", end="\n\n")
for i, result in enumerate(results):
    print(f"{i+1:3}. #{result['_id']:<20} {result['tweet_count']} tweets")
