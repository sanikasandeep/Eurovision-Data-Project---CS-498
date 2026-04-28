# Frontend with the help of AI
from flask import Flask, render_template
import pymongo

app = Flask(__name__)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["eurovision"]
collection = db["tweets"]
@app.route("/")

def dashboard():
    # Query 1 - Thread finder
    TARGET_USER = "blcklcfr"
    user_tweets = list(collection.find({"user.screen_name": TARGET_USER}))
    threads = []
    for tweet in user_tweets:
        if tweet.get("in_reply_to_status_id_str") is None:
            thread = []
            current = tweet
            while current is not None:
                thread.append(current)
                current = collection.find_one({
                    "in_reply_to_status_id_str": current["_id"],
                    "user.screen_name": TARGET_USER
                })
            thread.sort(key=lambda t: t.get("timestamp_ms", "0"))
            threads.append(thread)
          
    # Query 2 - Top countries
    q2_pipeline = [{"$match": {"place.country": {"$ne": None}}}, {"$group": {"_id": "$place.country", "tweet_count": {"$sum": 1}}}, {"$sort": {"tweet_count": -1}}, {"$limit": 10}]
    countries = list(collection.aggregate(q2_pipeline))
  
    # Query 3 - Most active user
    q3_pipeline = [
        {"$group": {
            "_id": "$user.screen_name",
            "name": {"$first": "$user.name"},
            "tweet_count": {"$sum": 1}
        }},
        {"$sort": {"tweet_count": -1}},
        {"$limit": 1}
    ]
    q3_result = list(collection.aggregate(q3_pipeline))
    top_user = q3_result[0] if q3_result else None

    # Query 4 - Top 100 hashtags
    q4_pipeline = [{"$unwind": "$entities.hashtags"}, {"$group": {"_id": "$entities.hashtags", "tweet_count": {"$sum": 1}}}, {"$sort": {"tweet_count": -1}}, {"$limit": 100}]
    hashtags = list(collection.aggregate(q4_pipeline))
    return render_template("dashboard.html", threads=threads, target_user=TARGET_USER, countries=countries, top_user=top_user, hashtags=hashtags)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
