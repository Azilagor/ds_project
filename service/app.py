from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime

# --- Подключение к Mongo ---
client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",
)
db = client["yoyoflot"]
col_passengers = db["mart_passengers"]
col_flights = db["data_unified"]

# --- FastAPI app ---
app = FastAPI(title="YoYoFlot Analytics API", version="1.0")

# --- CORS для фронта ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Главная страница ---
@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "YoYoFlot Analytics API",
        "time": datetime.utcnow(),
    }

# --- Пассажиры ---
@app.get("/api/passengers")
def get_all_passengers(limit: int = 50):
    """Получить список пассажиров (по умолчанию 50)"""
    data = list(
        col_passengers.find({}, {"_id": 0}).sort("last_name", 1).limit(limit)
    )
    return {"passengers": data, "count": len(data)}

@app.get("/api/passengers/search")
def search_passengers(query: str = Query(..., min_length=2)):
    """Поиск пассажиров по имени, фамилии или документу"""
    data = list(
        col_passengers.find(
            {"$text": {"$search": query}},
            {"score": {"$meta": "textScore"}, "_id": 0}
        ).sort([("score", {"$meta": "textScore"})]).limit(30)
    )
    return {"query": query, "results": data, "count": len(data)}

@app.get("/api/passengers/{document}")
def get_passenger(document: str):
    """Получить данные конкретного пассажира по документу"""
    p = col_passengers.find_one({"document": document}, {"_id": 0})
    if not p:
        return {"error": f"Passenger with document {document} not found"}
    return p




@app.get("/api/spy")
def find_spies(limit: int = 20):
    pipeline = [
        {"$match": {"passenger.document": {"$ne": None}}},
        {
            "$group": {
                "_id": "$passenger.document",
                "last_names": {"$addToSet": "$passenger.last_name"},
                "first_names": {"$addToSet": "$passenger.first_name"},
            }
        },
        # берём только тех, у кого фамилий больше одной
        {"$match": {"last_names.1": {"$exists": True}}},
        {
            "$project": {
                "_id": 0,
                "document": "$_id",
                "aliases": {
                    "$map": {
                        "input": {"$range": [0, {"$size": "$last_names"}]},
                        "as": "idx",
                        "in": {
                            "$concat": [
            { "$arrayElemAt": ["$first_names", {"$mod": ["$$idx", {"$size": "$first_names"}]}] },
            " ",
            { "$arrayElemAt": ["$first_names", {"$mod": ["$$idx", {"$size": "$first_names"}]}] }
        ]
                                }
                    }
                }
            }
        },
        {"$limit": limit}
    ]

    spies = list(db.data_unified.aggregate(pipeline))
    return {"spies": spies}



@app.get("/api/flights/all")
def get_all_flights():
    flights = db.data_unified.find({}, {"_id": 0, "flight.from_airport": 1, "flight.to_airport": 1})
    result = []
    for f in flights:
        fl = f.get("flight", {})
        if fl.get("from_airport") and fl.get("to_airport"):
            result.append({
                "from_airport": fl["from_airport"].strip(),
                "to_airport": fl["to_airport"].strip(),
            })
    return {"count": len(result), "flights": result}


@app.get("/api/flights/{document}")
def get_flights_by_document(document: str):
    flights = list(db.data_unified.find(
        {"passenger.document": document},
        {"_id": 0, "flight": 1, "ticket": 1}
    ))
    return {
        "document": document,
        "flights": [f["flight"] for f in flights if f.get("flight")],
        "count": len(flights)
    }


# --- Статистика ---
@app.get("/api/stats")
def get_stats():
    """Агрегированная статистика по пассажирам"""
    total_passengers = col_passengers.count_documents({})
    total_flights = db.data_unified.count_documents({})
    avg_flights = round(total_flights / max(total_passengers, 1), 2)
    most_active = list(
        col_passengers.find({}, {"_id": 0})
        .sort("total_flights", -1)
        .limit(5)
    )
    return {
        "total_passengers": total_passengers,
        "total_flights": total_flights,
        "avg_flights_per_passenger": avg_flights,
        "top_passengers": most_active,
        "generated_at": datetime.utcnow(),
    }

# --- Запуск ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081, reload=True)
