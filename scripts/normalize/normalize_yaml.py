from datetime import datetime
from pymongo import MongoClient

client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",
)

db = client["yoyoflot"]

def to_iso(date_str):
    """Преобразует дату в формат YYYY-MM-DD, если возможно."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

cleaned = []

for d in db["data_yaml"].find():
    # Приведение кодов аэропортов (в некоторых YAML может быть разный регистр)
    from_airport = (d.get("from") or "").strip().upper() or None
    to_airport = (d.get("to") or "").strip().upper() or None

    data = {
        "source": "yaml",
        "passenger_first_name": None,
        "passenger_middle_name": None,
        "passenger_last_name": None,
        "passenger_sex": None,
        "passenger_birth_date": None,
        "passenger_document": None,
        "booking_code": None,
        "ticket_number": None,
        "baggage": None,
        "flight_date": to_iso(d.get("date")),
        "flight_time": None,
        "flight_number": str(d.get("flight_number")) if d.get("flight_number") else None,
        "codeshare": None,
        "destination": to_airport,  # синхронизировано с to_airport
        "from_airport": from_airport,
        "to_airport": to_airport,
        "loyalty_program": d.get("ff_program") or None,
        "loyalty_number": d.get("ff_number") or None,
        "loyalty_status": d.get("ff_class") or None,
        "fare_code": d.get("ff_fare") or None,
        "status": (d.get("status") or "").capitalize() or None,
    }

    cleaned.append(data)

# Перезапись коллекции
db["normalized_yaml"].drop()
if cleaned:
    db["normalized_yaml"].insert_many(cleaned)

print(f"[+] normalized_yaml — {len(cleaned)} записей сохранено")
