import pandas as pd
from pymongo import MongoClient

client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",)

db = client["yoyoflot"]

def load_tab(path, collection_name):
    df = pd.read_csv(path, sep="\t")
    db[collection_name].insert_many(df.to_dict(orient="records"))
    print(f"[+] TAB loaded: {collection_name} ({len(df)} records)")

if __name__ == "__main__":
    load_tab("Airlines/Sirena-export-fixed.tab", "sirena")
