import pymongo

# connect to database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["eurovision"]
tweets = db["tweets"]

# We will use an aggregation pipeline that first groups by user.screen_name, then counts tweets per user
# outputs the top user to get the one with the most tweets. 
# The index on user.screen_name will be used to speed up grouping

pipeline = [
    {
        # 1. groups by user.screen_name and counts tweets/user
        "$group": {
            "_id": "$user.screen_name",
            "tweet_count": {"$sum": 1}
        }
    },
    {
        # 2. sort in descending order
        "$sort": {"tweet_count": -1}
    },
    {
        # 3. only outputs top result
        "$limit": 1
    }
]

result = list(tweets.aggregate(pipeline))

# output results
if result:
    print("User with most tweets:")
    print(result[0])
else:
    print("No results found")
