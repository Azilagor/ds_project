from pymongo import MongoClient
from datetime import datetime

client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",
)
db = client["yoyoflot"]

def to_iso(date_str):
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

cleaned = []

for d in db["data_tab"].find():
    data = {
        "source": "tab",
        "passenger_first_name": d.get("FirstName"),
        "passenger_middle_name": d.get("MiddleName"),
        "passenger_last_name": d.get("LastName"),
        "passenger_sex": None,
        "passenger_birth_date": to_iso(d.get("PaxBirthDate")),
        "passenger_document": None,
        "booking_code": None,
        "ticket_number": None,
        "baggage": None,
        "flight_date": to_iso(d.get("DepartDate")),
        "flight_time": d.get("DepartTime"),
        "arrival_date": to_iso(d.get("ArrivalDate")),
        "arrival_time": d.get("ArrivalTime"),
        "flight_number": d.get("FlightCode"),
        "codeshare": None,
        "destination": d.get("To"),
        "from_airport": d.get("From"),
        "to_airport": d.get("To"),
        "agent": d.get("Agent"),
    }
    cleaned.append(data)

db["normalized_tab"].drop()
if cleaned:
    db["normalized_tab"].insert_many(cleaned)

print(f"[+] normalized_tab — {len(cleaned)} записей сохранено")
