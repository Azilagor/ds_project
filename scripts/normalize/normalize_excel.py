
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

def to_iso(date_str):
    """Преобразует строку даты в формат YYYY-MM-DD или возвращает None."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

UNNEEDED_FIELDS = {"arrow", "seatlabel", "title", "note", "operator"}

cleaned = []

for d in db["data_excel"].find():
    # очищаем от мусорных полей
    doc = {k: v for k, v in d.items() if k not in UNNEEDED_FIELDS}

    # создаём нормализованный объект
    data = {
        "source": "excel",
        "passenger_first_name": d.get("PassengerFirstName"),
        "passenger_middle_name": d.get("PassengerSecondName"),
        "passenger_last_name": d.get("PassengerLastName"),
        "passenger_sex": (d.get("PassengerSex").capitalize()
                          if d.get("PassengerSex") else None),
        "passenger_birth_date": to_iso(d.get("PassengerBirthDate")),
        "passenger_document": d.get("PassengerDocument"),
        "booking_code": d.get("BookingCode"),
        "ticket_number": d.get("TicketNumber"),
        "baggage": d.get("Baggage"),
        "flight_date": to_iso(d.get("FlightDate")),
        "flight_time": d.get("FlightTime"),
        "flight_number": d.get("FlightNumber"),
        "codeshare": d.get("CodeShare"),
        "destination": d.get("Destination"),
        "from_airport": d.get("from") or None,
        "to_airport": d.get("to") or None,
        "seat_class": d.get("seatclass") or None,
        "seat": d.get("seat") or None,
        "ticket_type": d.get("tickettype") or None,
        "sourcefile": d.get("sourcefile") or None,
    }

    cleaned.append(data)

# перезаписываем коллекцию
db["normalized_excel"].drop()
if cleaned:
    db["normalized_excel"].insert_many(cleaned)

print(f"[+] normalized_excel — {len(cleaned)} записей сохранено")