import xml.etree.ElementTree as ET
from pymongo import MongoClient

tree = ET.parse("Airlines/PointzAggregator-AirlinesData.xml")
root = tree.getroot()

data = []

for user in root.findall("user"):
    uid = user.get("uid")
    name_elem = user.find("name")
    first = name_elem.get("first") if name_elem is not None else None
    last = name_elem.get("last") if name_elem is not None else None

    # перебираем карты
    cards_elem = user.find("cards")
    if cards_elem is not None:
        for card in cards_elem.findall("card"):
            card_number = card.get("number")
            bonus_program = card.findtext("bonusprogramm")

            activities = []
            acts = card.find("activities")
            if acts is not None:
                for act in acts.findall("activity"):
                    activities.append({
                        "type": act.get("type"),
                        "flight_code": act.findtext("Code"),
                        "date": act.findtext("Date"),
                        "departure": act.findtext("Departure"),
                        "arrival": act.findtext("Arrival"),
                        "fare": act.findtext("Fare")
                    })

            data.append({
                "uid": uid,
                "first_name": first,
                "last_name": last,
                "card_number": card_number,
                "bonus_program": bonus_program,
                "activities": activities
            })

print(f"Parsed {len(data)} user records")

# === Загрузка в MongoDB ===
client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",)
db = client["yoyoflot"]

db.airlines.drop()               # очищаем старую коллекцию
db.airlines.insert_many(data)    # загружаем новые
print("[+] Uploaded to MongoDB as 'airlines'")

