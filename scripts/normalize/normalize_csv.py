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

cleaned = []
for d in db["data_csv"].find():
    def to_iso(date_str):
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
        except Exception:
            return date_str  # если уже ISO

    data = {
        "source": "csv",
        "passenger_first_name": d.get("PassengerFirstName"),
        "passenger_middle_name": d.get("PassengerSecondName"),
        "passenger_last_name": d.get("PassengerLastName"),
        "passenger_sex": (d.get("PassengerSex") or "").capitalize() or None,
        "passenger_birth_date": to_iso(d.get("PassengerBirthDate")),
        "passenger_document": d.get("PassengerDocument"),
        "booking_code": d.get("BookingCode"),
        "ticket_number": d.get("TicketNumber"),
        "baggage": d.get("Baggage"),
        "flight_date": d.get("FlightDate"),
        "flight_time": d.get("FlightTime"),
        "flight_number": d.get("FlightNumber"),
        "codeshare": d.get("CodeShare"),
        "destination": d.get("Destination"),
    }
    cleaned.append(data)

db["normalized_csv"].drop()
if cleaned:
    db["normalized_csv"].insert_many(cleaned)

print(f"[+] normalized_csv — {len(cleaned)} записей сохранено")
