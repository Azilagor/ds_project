import json
from pymongo import MongoClient

client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot",)

db = client["yoyoflot"]


def load_json(path, collection_name):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # достаем список профилей
    if isinstance(data, dict) and "Forum Profiles" in data:
        records = data["Forum Profiles"]
    else:
        raise ValueError("Ожидался ключ 'Forum Profiles' в JSON")

    # грузим батчами по 100 документов
    batch_size = 100
    for i in range(0, len(records), batch_size):
        db[collection_name].insert_many(records[i:i+batch_size])

    print(f"[+] JSON loaded: {collection_name} ({len(records)} records)")

if __name__ == "__main__":
    load_json("Airlines/FrequentFlyerForum-Profiles.json", "forum_profiles")
