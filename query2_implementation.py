import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["eurovision"]
collection = db["tweets"]

# We will use an aggregation pipeline that first filters out “null” place values, group by place.country
# and use the index on place.country to speed up grouping 

pipeline = [
    {
        # 1. filter out null / missing countries
        "$match": {
            "place.country": {"$ne": None}
        }
    },
    {
        # 2. group by country and count tweets
        "$group": {
            "_id": "$place.country",
            "tweet_count": {"$sum": 1}
        }
    },
    {
        # 3. sort descending by tweet_count
        "$sort": {
            "tweet_count": -1
        }
    },
    {
        # 4. only use top country
        "$limit": 1
    }
]

# run aggregation pipeline
result = list(collection.aggregate(pipeline))

# output result
if result:
    top_country = result[0]
    print("Top country by tweet count:")
    print(f"Country: {top_country['_id']}")
    print(f"Tweet count: {top_country['tweet_count']}")
else:
    print("No results found.")
