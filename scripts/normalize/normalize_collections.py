from pymongo import MongoClient
from datetime import datetime, timezone

client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",
)
db = client["yoyoflot"]

collections = [
    "normalized_csv",
    "normalized_excel",
    "normalized_tab",
    "normalized_json",
    "normalized_yaml",
]

target = db["data_unified"]

# не дропаем полностью, если уже есть записи
if "data_unified" not in db.list_collection_names():
    print("🆕 Создаём новую коллекцию data_unified")
else:
    print("⚠️ Коллекция уже существует — будем дозаписывать")

BATCH_SIZE = 1000
total_inserted = 0

def clean_dict(d):
    return {k: v for k, v in d.items() if v not in [None, "", [], {}]}

for name in collections:
    source_col = db[name]
    count = source_col.count_documents({})
    print(f"\n📂 {name} — {count} документов")

    cursor = source_col.find()
    batch = []

    for d in cursor:
        doc = {
            "source": d.get("source"),

            "passenger": clean_dict({
                "first_name": d.get("passenger_first_name"),
                "middle_name": d.get("passenger_middle_name"),
                "last_name": d.get("passenger_last_name"),
                "sex": d.get("passenger_sex"),
                "birth_date": d.get("passenger_birth_date"),
                "document": d.get("passenger_document"),
            }),

            "flight": clean_dict({
                "number": d.get("flight_number"),
                "date": d.get("flight_date"),
                "time": d.get("flight_time"),
                "from_airport": d.get("from_airport"),
                "to_airport": d.get("to_airport") or d.get("destination"),
                "destination": d.get("destination"),
                "codeshare": d.get("codeshare"),
                "agent": d.get("agent"),
            }),

            "ticket": clean_dict({
                "booking_code": d.get("booking_code"),
                "ticket_number": d.get("ticket_number"),
                "ticket_type": d.get("ticket_type"),
                "seat_class": d.get("seat_class"),
                "seat": d.get("seat"),
                "baggage": d.get("baggage"),
            }),

            "loyalty": clean_dict({
                "program": d.get("loyalty_program"),
                "number": d.get("loyalty_number"),
                "status": d.get("loyalty_status"),
            }),

            "meta": {
                "source_file": d.get("sourcefile"),
                "status": d.get("status"),
                "inserted_at": datetime.now(timezone.utc),
            }
        }

        batch.append(doc)

        # вставляем порциями
        if len(batch) >= BATCH_SIZE:
            target.insert_many(batch)
            total_inserted += len(batch)
            print(f"✅ Вставлено {total_inserted} документов...")
            batch = []

    # вставляем оставшиеся
    if batch:
        target.insert_many(batch)
        total_inserted += len(batch)
        print(f"✅ Вставлено {total_inserted} документов...")

print(f"\n🎯 Готово! Всего д    обавлено: {total_inserted} документов в data_unified ✅")
