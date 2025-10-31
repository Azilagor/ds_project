# find_multiple_passports.py
from pymongo import MongoClient
import re
from collections import defaultdict

# Настройки подключения — подставь свои креды если нужно
client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot",
    )
db = client["yoyoflot"]

# Ключи, которые считаем именами
FIRST_KEYS = {"first_name", "firstname", "passengerfirstname", "FirstName", "PassengerFirstName"}
LAST_KEYS  = {"last_name", "lastname", "passengerlastname", "PassengerLastName", "LastName"}
MIDDLE_KEYS = {"middle_name", "middlename", "passengersecondname", "PassengerSecondName"}

# Регекс для поиска полей-подозрений (passport, pass, document, doc, travel)
FIELD_RE = re.compile(r"(pass|passport|document|doc|travel)", re.I)

def find_passport_values(obj):
    """Рекурсивно собрать все значения полей, совпадающих с FIELD_RE"""
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if FIELD_RE.search(str(k)):
                # нормализация строки
                if isinstance(v, (str, int)):
                    s = str(v).strip()
                    if s:
                        found.append(s)
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, (str, int)):
                            s = str(item).strip()
                            if s:
                                found.append(s)
                        elif isinstance(item, dict):
                            found.extend(find_passport_values(item))
                elif isinstance(v, dict):
                    found.extend(find_passport_values(v))
            else:
                # рекурсия
                if isinstance(v, dict):
                    found.extend(find_passport_values(v))
                elif isinstance(v, list):
                    for it in v:
                        if isinstance(it, dict):
                            found.extend(find_passport_values(it))
    return found

def extract_name(obj):
    """Попробовать извлечь ФИО из документа по разным ключам"""
    first = None; last = None; middle = None
    for k, v in obj.items():
        kl = k.lower()
        if kl in FIRST_KEYS and isinstance(v, str):
            first = v.strip().upper()
        elif kl in LAST_KEYS and isinstance(v, str):
            last = v.strip().upper()
        elif kl in MIDDLE_KEYS and isinstance(v, str):
            middle = v.strip().upper()
    # Попытка взять из вложенных 'name' или 'Real Name' структуры
    if not first or not last:
        # проверяем вложенные поля common like name: { first, last } or name first/last attributes
        for k, v in obj.items():
            if isinstance(v, dict):
                if not first and any(x in v for x in ["first", "first_name", "firstname"]):
                    for kk, vv in v.items():
                        if kk.lower().startswith("first") and isinstance(vv, str):
                            first = vv.strip().upper()
                if not last and any(x in v for x in ["last", "last_name", "lastname"]):
                    for kk, vv in v.items():
                        if kk.lower().startswith("last") and isinstance(vv, str):
                            last = vv.strip().upper()
    return first or "", last or "", middle or ""

# map: (first,last) -> set of passport strings (normalized)
people_passports = defaultdict(set)

collections = db.list_collection_names()

for coll_name in collections:
    coll = db[coll_name]
    # iterate docs (consider limiting if huge)
    for doc in coll.find({}, no_cursor_timeout=True):
        try:
            passports = find_passport_values(doc)
            if not passports:
                continue
            first, last, middle = extract_name(doc)
            # Если нет имени — попытаемся достать из других полей
            if not (first or last):
                # try parse single full name fields
                for k in doc:
                    if "name" in k.lower():
                        v = doc.get(k)
                        if isinstance(v, str):
                            parts = v.split()
                            if len(parts) >= 2:
                                first = parts[0].strip().upper()
                                last = parts[-1].strip().upper()
                                break
            # нормализуем номера паспортов: убираем пробелы, приводим к верхнему
            normalized = set()
            for p in passports:
                p_norm = re.sub(r"\s+", "", p).upper()
                if p_norm:
                    normalized.add(p_norm)
            key = (first, last) if (first or last) else ("__UNKNOWN__", coll_name)
            for p in normalized:
                people_passports[key].add(p)
        except Exception:
            # не ломаемся на одном документе
            continue
    # безопасно close cursor handled by driver

# Теперь отфильтруем людей с более чем 1 уникальным паспортом
multi_pass = [(name, sorted(list(ps))) for name, ps in people_passports.items() if len(ps) > 1]

# Выведем результат
print("Найдено людей с >1 уникальным паспортом:", len(multi_pass))
for (first, last), passports in sorted(multi_pass, key=lambda x: (-len(x[1]), x[0])):
    print(f"{first} {last} — passports: {passports}")
