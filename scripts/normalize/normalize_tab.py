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
    """Преобразует строку даты в ISO формат YYYY-MM-DD, если возможно."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

cleaned = []

for d in db["data_tab"].find():
    data = {
        "source": "tab",
        "passenger_first_name": d.get("FirstName"),
        "passenger_middle_name": d.get("MiddleName"),
        "passenger_last_name": d.get("LastName"),
        "passenger_sex": (d.get("Sex").capitalize() if d.get("Sex") else None)
                         if "Sex" in d else None,
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

# перезаписываем коллекцию
db["normalized_tab"].drop()
if cleaned:
    db["normalized_tab"].insert_many(cleaned)

print(f"[+] normalized_tab — {len(cleaned)} записей сохранено")