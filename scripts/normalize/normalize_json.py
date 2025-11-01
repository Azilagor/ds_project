from datetime import datetime
from pymongo import MongoClient

client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
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

for d in db["data_json"].find():
    sex = (d.get("sex") or "").strip().capitalize() or None
    flight_number = str(d.get("flight")) if d.get("flight") else None

    data = {
        "source": "json",
        "passenger_first_name": d.get("first_name"),
        "passenger_middle_name": None,
        "passenger_last_name": d.get("last_name"),
        "passenger_sex": sex,
        "passenger_birth_date": None,
        "passenger_document": None,
        "booking_code": None,
        "ticket_number": None,
        "baggage": None,
        "flight_date": to_iso(d.get("date")),
        "flight_time": None,
        "flight_number": flight_number,
        "codeshare": "Operated" if d.get("codeshare") else "Own",
        "destination": d.get("to_city"),
        "from_airport": d.get("from_airport"),
        "to_airport": d.get("to_airport"),
        "loyalty_program": d.get("loyalty_program") or None,
        "loyalty_number": d.get("loyalty_number") or None,
        "loyalty_status": d.get("loyalty_status") or None,
    }

    cleaned.append(data)

db["normalized_json"].drop()
if cleaned:
    db["normalized_json"].insert_many(cleaned)

print(f"[+] normalized_json — {len(cleaned)} записей сохранено")
