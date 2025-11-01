import pandas as pd
from pymongo import MongoClient

client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot",)

db = client["yoyoflot"]

def load_csv(path, collection_name):
    df = pd.read_csv(path)
    
    # Преобразуем все имена столбцов в строки
    df.columns = df.columns.map(str)
    
    # Преобразуем NaN → None (Mongo не принимает NaN)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")
    if not records:
        print(f"[!] CSV is empty: {path}")
        return
    
    # Очищаем коллекцию перед загрузкой
    db[collection_name].delete_many({})
    result = db[collection_name].insert_many(records)
    
    print(f"[+] CSV reloaded: {collection_name} ({len(result.inserted_ids)} records)")

if __name__ == "__main__":
    load_csv("../Airlines/BoardingData.csv", "boardingdata")


