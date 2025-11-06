from pymongo import MongoClient
import pandas as pd

# Подключение
client = MongoClient("mongodb://ds_user:@:27017/yoyoflot?authSource=yoyoflot")
db = client["yoyoflot"]
col = db["timetable"]

# Запросы для двух направлений
query_forward = {"from.code": "AAL", "to.code": "AMS"}
query_backward = {"from.code": "AMS", "to.code": "AAL"}

# Получаем документы
forward = list(col.find(query_forward, {"_id": 0}))
backward = list(col.find(query_backward, {"_id": 0}))

print(f"🛫 AAL → AMS: {len(forward)} рейсов")
print(f"🛬 AMS → AAL: {len(backward)} рейсов")

# Преобразуем в DataFrame
df_fwd = pd.DataFrame(forward)
df_bwd = pd.DataFrame(backward)

# Добавим метку направления
df_fwd["direction"] = "AAL→AMS"
df_bwd["direction"] = "AMS→AAL"

# Объединим и отсортируем
df = pd.concat([df_fwd, df_bwd], ignore_index=True)
df = df.sort_values(by=["direction", "validity", "dep_time"]).reset_index(drop=True)

# Сохраним в CSV
df.to_csv("timetable_check.csv", index=False, encoding="utf-8")
print("✅ Файл сохранён: timetable_check.csv")

# Показать первые 10 строк
print("\nПример данных:")
print(df.head().to_string(index=False))
