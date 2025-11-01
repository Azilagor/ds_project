from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime

# --- Подключение к Mongo ---
client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
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

# --- Рейсы ---
@app.get("/api/flights/{document}")
def get_flights_by_passenger(document: str):
    """Список рейсов по документу"""
    flights = list(
        col_flights.find(
            {"passenger.document": document},
            {"_id": 0, "flight": 1, "ticket": 1}
        )
    )
    data = []
    for f in flights:
        flight = f.get("flight", {})
        ticket = f.get("ticket", {})
        flight.update({"ticket": ticket})
        data.append(flight)
    return {"document": document, "flights": data, "count": len(data)}

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
