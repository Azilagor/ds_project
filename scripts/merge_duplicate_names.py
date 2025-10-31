from pymongo import MongoClient

client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot",)

db = client["yoyoflot"]

seen = set()
deleted = 0

cursor = db.data_csv.find({}, {"_id": 1, "PassengerDocument": 1, "PassengerFirstName": 1, "PassengerLastName": 1})

for doc in cursor:
    doc_id = doc["_id"]
    passport = doc.get("PassengerDocument")
    first = doc.get("PassengerFirstName")
    last = doc.get("PassengerLastName")

    if not (passport and first and last):
        continue

    key = f"{passport.strip()}_{first.strip().upper()}_{last.strip().upper()}"

    if key in seen:
        db.data_csv.delete_one({"_id": doc_id})
        deleted += 1
    else:
        seen.add(key)

print(f"✅ Удалено {deleted} дубликатов из data_csv")
