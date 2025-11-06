from pymongo import MongoClient

client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",)

db = client["yoyoflot"]

cleaned = []

for doc in db["exchange_flights"].find():
    base = {
        "flight_number": doc.get("flight_number"),
        "date": doc.get("date"),
        "from": doc.get("from"),
        "to": doc.get("to"),
        "status": doc.get("status"),
    }

    # массив ff разворачиваем по одной записи
    ffs = doc.get("ff", [])
    if not ffs:
        cleaned.append(base)
    else:
        for ff in ffs:
            rec = base.copy()
            rec.update({
                "ff_program": ff.get("program"),
                "ff_number": ff.get("number"),
                "ff_class": ff.get("class"),
                "ff_fare": ff.get("fare"),
            })
            cleaned.append(rec)

# перезаписываем в новую коллекцию
db["exchange_flights_clean"].drop()
if cleaned:
    db["exchange_flights_clean"].insert_many(cleaned)

print(f"[+] Cleaned {len(cleaned)} flight records saved to exchange_flights_clean")
