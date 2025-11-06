from pymongo import MongoClient
import pandas as pd

client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",
    )

db = client["yoyoflot"]

collections = [
    "airlines",
    "boardingdata",
    "exchange_flights",
    "forum_profiles",
    "sirena",
    "yoyoflot",
]

dfs = {col: pd.DataFrame(list(db[col].find())) for col in collections}
for name, df in dfs.items():
    print(f"{name}: {len(df)} records, columns={list(df.columns)}")
