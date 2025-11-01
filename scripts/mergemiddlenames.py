from pymongo import MongoClient

client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot",
)
db = client["yoyoflot"]

def is_short(m):
    return m and len(m.strip()) <= 2 and m.endswith(".")

def is_full(m):
    return m and len(m.strip()) > 3 and not m.endswith(".")

collection = db["normalized_csv"]
updated = 0
batch = 0

print("\n🔹 Scanning collection normalized_csv...")

pipeline = [
    {"$match": {"passenger_document": {"$ne": None}}},
    {"$group": {
        "_id": "$passenger_document",
        "names": {"$addToSet": "$passenger_middle_name"},
        "count": {"$sum": 1}
    }},
    {"$match": {"count": {"$gt": 1}}}
]

cursor = collection.aggregate(pipeline, allowDiskUse=True)

for g in cursor:
    mids = [m for m in g["names"] if m]
    full = next((m for m in mids if is_full(m)), None)
    if not full:
        continue

    shorts = [m for m in mids if is_short(m)]
    if not shorts:
        continue

    res = collection.update_many(
        {"passenger_document": g["_id"], "passenger_middle_name": {"$in": shorts}},
        {"$set": {"passenger_middle_name": full}}
    )
    if res.modified_count:
        updated += res.modified_count
        batch += 1
        if batch % 20 == 0:
            print(f"  ✅ {updated} updated so far...")

print(f"\n🎯 Done. Total updated: {updated}")
