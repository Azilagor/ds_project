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

updated = 0

for coll_name in [
    "normalized_csv",
]:
    collection = db[coll_name]
    print(f"\n🔹 Checking {coll_name}...")

    # группируем по документу
    pipeline = [
        {"$match": {"passenger_document": {"$ne": None}}},
        {"$group": {
            "_id": "$passenger_document",
            "names": {"$addToSet": "$passenger_middle_name"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]

    groups = list(collection.aggregate(pipeline))

    for g in groups:
        mids = [m for m in g["names"] if m]
        full = next((m for m in mids if is_full(m)), None)
        if not full:
            continue
        # обновляем все сокращённые
        res = collection.update_many(
            {"passenger_document": g["_id"], "passenger_middle_name": {"$in": [m for m in mids if is_short(m)]}},
            {"$set": {"passenger_middle_name": full}}
        )
        if res.modified_count:
            print(f"  {res.modified_count} → updated for {g['_id']} ({full})")
            updated += res.modified_count

print(f"\n✅ Всего обновлено {updated} записей по всем коллекциям.")
