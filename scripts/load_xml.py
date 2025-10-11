import xml.etree.ElementTree as ET
from pymongo import MongoClient

client = MongoClient("mongodb://ds_user:StrongPassword123@185.22.67.9:27017/yoyoflot?authSource=yoyoflot")
db = client["yoyoflot"]

def load_xml(path, collection_name):
    tree = ET.parse(path)
    root = tree.getroot()
    records = []
    for item in root:
        record = {child.tag: child.text for child in item}
        records.append(record)
    db[collection_name].insert_many(records)
    print(f"[+] XML loaded: {collection_name} ({len(records)} records)")

if __name__ == "__main__":
    load_xml("Airlines/PointzAggregator-AirlinesData.xml", "airlines")
