from pymongo import MongoClient

client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",)

db = client["yoyoflot"]

cleaned = []
expected_len = 14

for doc in db["boardingdata"].find():
    raw_value = list(doc.values())[1]
    parts = raw_value.split(";")

    # если строка короче — дополним None, если длиннее — обрежем
    if len(parts) < expected_len:
        parts += [None] * (expected_len - len(parts))
    elif len(parts) > expected_len:
        parts = parts[:expected_len]

    data = {
        "PassengerFirstName": parts[0] or None,
        "PassengerSecondName": parts[1] or None,
        "PassengerLastName": parts[2] or None,
        "PassengerSex": parts[3] or None,
        "PassengerBirthDate": parts[4] or None,
        "PassengerDocument": parts[5] or None,
        "BookingCode": parts[6] or None,
        "TicketNumber": parts[7] or None,
        "Baggage": parts[8] or None,
        "FlightDate": parts[9] or None,
        "FlightTime": parts[10] or None,
        "FlightNumber": parts[11] or None,
        "CodeShare": parts[12] or None,
        "Destination": parts[13] or None,
    }
    cleaned.append(data)

# пересоздаём коллекцию
db["data_csv"].drop()
if cleaned:
    db["data_csv"].insert_many(cleaned)

print(f"[+] Parsed and saved {len(cleaned)} records to data_csv")
