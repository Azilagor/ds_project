
from pymongo import MongoClient

client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot",)

db = client["yoyoflot"]

cleaned = []

for doc in db["boardingdata"].find():
    raw_value = list(doc.values())[1]
    parts = raw_value.split(";")
    if len(parts) != 14:
        continue  # пропускаем странные строки
    
    data = {
        "PassengerFirstName": parts[0],
        "PassengerSecondName": parts[1],
        "PassengerLastName": parts[2],
        "PassengerSex": parts[3],
        "PassengerBirthDate": parts[4],
        "PassengerDocument": parts[5],
        "BookingCode": parts[6],
        "TicketNumber": parts[7],
        "Baggage": parts[8],
        "FlightDate": parts[9],
        "FlightTime": parts[10],
        "FlightNumber": parts[11],
        "CodeShare": parts[12],
        "Destination": parts[13],
    }
    cleaned.append(data)

db["data_csv"].drop()
if cleaned:
    db["data_csv"].insert_many(cleaned)

print(f"[+] Parsed and saved {len(cleaned)} records to data_boarding")
