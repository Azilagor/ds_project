from pymongo import MongoClient
from datetime import datetime


db = client["yoyoflot"]

collections = ["data_xml", "data_yaml", "data_json", "data_tab", "data_csv"]

# === Вспомогательные функции ===
def normalize_date(date_str):
    """Приводим даты к ISO-формату YYYY-MM-DD"""
    if not date_str or not isinstance(date_str, str):
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def upper_or_none(value):
    """Безопасно поднимаем регистр"""
    return value.strip().upper() if isinstance(value, str) else None

# === Универсальный нормализатор ===
def normalize_doc(doc):
    base = {
        "first_name": None,
        "last_name": None,
        "middle_name": None,
        "flight_code": None,
        "date": None,
        "from_airport": None,
        "to_airport": None,
        "loyalty_program": None,
        "loyalty_number": None,
        "agent": None,
        "ticket_number": None,
        "destination": None,
        "sex": None,
    }

    # === Плоские ключи ===
    for k, v in doc.items():
        if not v:
            continue
        key = k.lower()

        # Имена
        if key in ["first_name", "passengerfirstname", "firstname"]:
            base["first_name"] = upper_or_none(v)
        elif key in ["last_name", "passengerlastname", "lastname"]:
            base["last_name"] = upper_or_none(v)
        elif key in ["middle_name", "passengersecondname", "middlename"]:
            base["middle_name"] = upper_or_none(v)
        elif key in ["sex", "passengersex"]:
            base["sex"] = v.capitalize()

        # Полёты
        elif "flight" in key and "number" in key:
            base["flight_code"] = upper_or_none(v)
        elif key in ["flightcode", "flight", "flight_number"]:
            base["flight_code"] = upper_or_none(v)
        elif key in ["departdate", "flightdate", "date"]:
            base["date"] = normalize_date(v)
        elif key in ["from", "from_airport", "departure"]:
            base["from_airport"] = upper_or_none(v)
        elif key in ["to", "to_airport", "arrival", "destination"]:
            base["to_airport"] = upper_or_none(v)
        elif key == "destination":
            base["destination"] = upper_or_none(v)

        # Лояльность и документы
        elif key in ["bonus_program", "ff_program", "loyalty_program"]:
            base["loyalty_program"] = upper_or_none(v)
        elif key in ["card_number", "ff_number", "loyalty_number"]:
            base["loyalty_number"] = v.strip()
        elif key == "agent":
            base["agent"] = upper_or_none(v)
        elif "ticket" in key:
            base["ticket_number"] = v.strip()

    docs = []

    # === XML ===
    if "activities" in doc and isinstance(doc["activities"], list):
        for act in doc["activities"]:
            new_entry = base.copy()
            new_entry.update({
                "flight_code": upper_or_none(act.get("flight_code")),
                "date": normalize_date(act.get("date")),
                "from_airport": upper_or_none(act.get("departure")),
                "to_airport": upper_or_none(act.get("arrival")),
            })
            docs.append({k: v for k, v in new_entry.items() if v})

    # === JSON ===
    elif "Registered Flights" in doc and isinstance(doc["Registered Flights"], list):
        for fl in doc["Registered Flights"]:
            new_entry = base.copy()
            new_entry.update({
                "flight_code": upper_or_none(fl.get("Flight")),
                "date": normalize_date(fl.get("Date")),
                "from_airport": upper_or_none(fl.get("Departure", {}).get("Airport")),
                "to_airport": upper_or_none(fl.get("Arrival", {}).get("Airport")),
            })
            docs.append({k: v for k, v in new_entry.items() if v})

    # Если нет вложенных структур
    if not docs:
        docs.append({k: v for k, v in base.items() if v})

    return docs

# === Очистим и создадим коллекцию ===
db["data_normalized"].drop()

total = 0
for coll_name in collections:
    coll = db[coll_name]
    cleaned = []
    for doc in coll.find():
        normalized_docs = normalize_doc(doc)
        for n in normalized_docs:
            if n:
                cleaned.append(n)

    if cleaned:
        db["data_normalized"].insert_many(cleaned)
        print(f"[+] {coll_name}: добавлено {len(cleaned)} нормализованных записей")
        total += len(cleaned)

print(f"\n✅ Всего нормализовано {total} документов в коллекцию data_normalized")