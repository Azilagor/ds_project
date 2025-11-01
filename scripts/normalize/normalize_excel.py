from pymongo import MongoClient
from datetime import datetime

client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot",
)
db = client["yoyoflot"]

def to_iso(date_str):
    """Преобразует дату в ISO формат YYYY-MM-DD"""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

cleaned = []

for d in db["data_excel"].find():
    # Разбор имени "Lidiya Zhdanova"
    full_name = d.get("passenger") or ""
    parts = full_name.split()
    first_name = parts[0] if len(parts) > 0 else None
    last_name = parts[1] if len(parts) > 1 else None

    data = {
        "source": "excel",
        "passenger_first_name": first_name,
        "passenger_middle_name": None,
        "passenger_last_name": last_name,
        "passenger_sex": None,
        "passenger_birth_date": None,
        "passenger_document": None,
        "booking_code": d.get("pnr"),
        "ticket_number": d.get("ticketnumber"),
        "baggage": None,
        "flight_date": to_iso(d.get("date")),
        "flight_time": d.get("time"),
        "flight_number": d.get("flight"),
        "codeshare": d.get("operator"),
        "destination": d.get("to"),
        "from_airport": d.get("from"),
        "to_airport": d.get("to"),
        "seat_class": d.get("seatclass"),
        "seat": d.get("seat"),
        "ticket_type": d.get("tickettype"),
        "sourcefile": d.get("sourcefile"),
    }

    cleaned.append(data)

db["normalized_excel"].drop()
if cleaned:
    db["normalized_excel"].insert_many(cleaned)

print(f"[+] normalized_excel — {len(cleaned)} записей сохранено")
