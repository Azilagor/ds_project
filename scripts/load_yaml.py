import os
import re
import yaml
from pymongo import MongoClient

client = MongoClient("mongodb://ds_user:StrongPassword123@185.22.67.9:27017/yoyoflot?authSource=yoyoflot")
db = client["yoyoflot"]

# ---------- Paths ----------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "Airlines")
DEFAULT_YAML = os.path.join(DATA_DIR, "SkyTeam-Exchange.yaml")  # поменяй при необходимости

# ---------- Utils ----------
TOPLEVEL_KEY_RE = re.compile(r"^(?P<key>(\"[^\"]+\"|'[^']+'|[^:\n#]+)):\s*$")

def iter_top_blocks(yaml_path):
    """
    Итерируемся по верхнеуровневым блокам YAML (ключи без отступов),
    буферим строки до следующего такого ключа и отдаём как отдельный YAML-фрагмент.
    Это даёт потоковую обработку 'день за днём'.
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        buf = []
        for line in f:
            # новая "дата" начинается с 0-го столбца и содержит двоеточие
            if (not line.startswith((" ", "\t"))) and TOPLEVEL_KEY_RE.match(line):
                if buf:
                    yield "".join(buf)
                    buf = [line]
                else:
                    buf.append(line)
            else:
                buf.append(line)
        if buf:
            yield "".join(buf)

def normalize_ff(ff_mapping):
    """ FF:
          'FB 520518073': {CLASS: Y, FARE: YRSTFN}
        -> [{'program':'FB','number':'520518073','CLASS':'Y','FARE':'YRSTFN'}, ...]
    """
    if not isinstance(ff_mapping, dict):
        return []
    items = []
    for k, v in ff_mapping.items():
        # ключ вида "FB 520518073"
        parts = str(k).split(None, 1)
        program = parts[0].strip() if parts else None
        number = parts[1].strip() if len(parts) > 1 else None
        rec = {"program": program, "number": number}
        if isinstance(v, dict):
            for vk, vv in v.items():
                rec[str(vk).lower()] = vv
        items.append(rec)
    return items

def to_doc(date_key, flight_no, details):
    """
    Превращаем один рейс в плоский документ для MongoDB.
    details пример:
      {
        'FF': {...}, 'FROM':'SVO', 'TO':'CDG', 'STATUS':'LANDED'
      }
    """
    if details is None:
        details = {}
    ff_list = normalize_ff(details.get("FF"))
    doc = {
        "date": str(date_key).strip().strip("'").strip('"'),
        "flight_number": str(flight_no).strip(),
        "from": details.get("FROM"),
        "to": details.get("TO"),
        "status": details.get("STATUS"),
        "ff": ff_list,
    }
    # на всякий случай приложим сырой блок (без FF — он уже развёрнут)
    raw_copy = {k: v for k, v in details.items() if k != "FF"}
    doc["raw"] = raw_copy
    return doc

def load_yaml_streaming(yaml_path, collection_name, batch_size=1000):
    """
    Стриминговая загрузка: читаем по одному дню, парсим его,
    превращаем каждый рейс в документ, вставляем батчами.
    """
    col = db[collection_name]
    batch = []
    day_counter = 0
    flight_counter = 0

    for chunk in iter_top_blocks(yaml_path):
        try:
            day_map = yaml.safe_load(chunk)
        except Exception as e:
            print(f"[!] YAML parse error, пропускаю блок:\n{e}")
            continue

        if not isinstance(day_map, dict):
            continue

        # в каждом блоке ожидаем одну пару: { '2017-01-01': { AF1145: {...}, ... } }
        for date_key, flights in day_map.items():
            day_counter += 1
            if not isinstance(flights, dict):
                continue

            for flight_no, details in flights.items():
                batch.append(to_doc(date_key, flight_no, details))
                flight_counter += 1

                if len(batch) >= batch_size:
                    col.insert_many(batch)
                    print(f"[+] Inserted batch: {len(batch)} (total flights: {flight_counter})")
                    batch.clear()

    if batch:
        col.insert_many(batch)
        print(f"[+] Inserted last batch: {len(batch)}")

    print(f"✅ Done. Days parsed: {day_counter}, flights inserted: {flight_counter}, collection: {collection_name}")

if __name__ == "__main__":
    # можно менять путь/коллекцию при запуске
    load_yaml_streaming(DEFAULT_YAML, "exchange_flights", batch_size=1000)