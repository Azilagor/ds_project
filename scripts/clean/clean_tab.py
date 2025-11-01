import re
from pymongo import MongoClient

client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot",
    )

db = client["yoyoflot"]

# Регулярка — адаптирована под формат Sirena Travel (гибкая)
pattern = re.compile(
    r"^(?P<PaxName>[А-ЯЁA-Z\s]+)\s+"
    r"(?P<PaxBirthDate>(?:\d{4}-\d{2}-\d{2}|N/A))\s+"
    r"(?P<DepartDate>\d{4}-\d{2}-\d{2})\s+"
    r"(?P<DepartTime>\d{2}:\d{2})\s+"
    r"(?P<ArrivalDate>\d{4}-\d{2}-\d{2})\s+"
    r"(?P<ArrivalTime>\d{2}:\d{2})\s+"
    r"(?P<FlightCode>[A-Z]{2}\d+)\w*\s+"
    r"(?P<From>[A-Z]{3})\s+(?P<To>[A-Z]{3}).*?"
    r"(FF#(?P<FFProgram>[A-Z]{2})\s*(?P<FFNumber>\d+))?.*?"
    r"(?P<Agent>[A-Za-zА-Яа-я0-9]+)?$"
)

cleaned = []
for doc in db["sirena"].find():
    # в документе одно длинное поле, берём второе значение
    line = list(doc.values())[1].strip()
    match = pattern.search(line)
    if not match:
        continue

    data = match.groupdict()
    full_name = data.pop("PaxName", "").strip()
    parts = full_name.split()
    data["LastName"] = parts[0] if len(parts) > 0 else None
    data["FirstName"] = parts[1] if len(parts) > 1 else None
    data["MiddleName"] = parts[2] if len(parts) > 2 else None

    # чистим от None и пустых строк
    data = {k: v.strip() if isinstance(v, str) else v for k, v in data.items() if v not in [None, "", "N/A"]}
    cleaned.append(data)

db["siren_clean"].drop()
if cleaned:
    db["siren_clean"].insert_many(cleaned)

print(f"[+] Parsed and saved {len(cleaned)} records to siren_clean")
