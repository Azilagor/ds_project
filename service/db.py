from pymongo import MongoClient

def get_db():
    client = MongoClient(
        host="185.22.67.9",
        port=27017,
        username="yoyoadmin",
        password="YoyoFlotslzL6A8ekU",
        authSource="yoyoflot",
    )
    return client["yoyoflot"]
