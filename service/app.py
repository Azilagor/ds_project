from flask import Flask, jsonify, request
from db import get_db

app = Flask(__name__)
db = get_db()
col = db["data_normalized"]

@app.route("/people")
def get_people():
    """Список уникальных пассажиров"""
    pipeline = [
        {"$group": {"_id": {"first": "$first_name", "last": "$last_name"}}},
        {"$match": {"_id.first": {"$ne": None}, "_id.last": {"$ne": None}}},
        {"$limit": 100}
    ]
    people = [
        {"first_name": p["_id"]["first"], "last_name": p["_id"]["last"]}
        for p in col.aggregate(pipeline)
    ]
    return jsonify(people)

@app.route("/flights")
def get_flights():
    """Все рейсы или только выбранного пассажира"""
    first = request.args.get("first_name")
    last = request.args.get("last_name")

    query = {}
    if first and last:
        query = {"first_name": first.upper(), "last_name": last.upper()}

    projection = {"_id": 0, "from_airport": 1, "to_airport": 1, "date": 1}
    flights = list(col.find(query, projection))
    return jsonify(flights)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=True)
