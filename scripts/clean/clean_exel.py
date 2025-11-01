import os
import json
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# === 1. Подключение к Mongo ===
client = MongoClient(
    host="185.22.67.9",
    port=27017,
    username="yoyoadmin",
    password="YoyoFlotslzL6A8ekU",
    authSource="yoyoflot"
)
db = client["yoyoflot"]
collection = db["boarding_passes_clean"]

# === 2. Папка с JSON-файлами ===
INPUT_FOLDER = "./Processed"
files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.endswith("_c.json")])

if not files:
    print("❌ В папке 'Processed' нет файлов *_c.json")
    exit()

print(f"🚀 Найдено {len(files)} файлов для загрузки\n")

start_time = datetime.now()
total_inserted = 0

# === 3. Обрабатываем каждый JSON-файл ===
for idx, file_name in enumerate(files, 1):
    file_path = os.path.join(INPUT_FOLDER, file_name)
    print(f"[{idx}/{len(files)}] 📄 Обработка: {file_name}")

    try:
        # === Читаем JSON ===
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not data:
            print(f"⚠️ Пропуск: файл {file_name} пустой")
            continue

        df = pd.DataFrame(data)

        # === Очистка и нормализация ===
        df.drop_duplicates(inplace=True)
        df = df.where(pd.notnull(df), None)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        if "date" in df.columns and "time" in df.columns:
            df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"], errors="coerce")

        if "passenger" in df.columns:
            df["passenger"] = df["passenger"].astype(str).str.title().str.strip()
        if "from" in df.columns:
            df["from"] = df["from"].astype(str).str.title()
        if "to" in df.columns:
            df["to"] = df["to"].astype(str).str.title()
        if "operator" in df.columns:
            df["operator"] = df["operator"].astype(str).str.replace("Operated by ", "", regex=False)

        # === Вставляем в Mongo ===
        records = df.to_dict(orient="records")
        if records:
            result = collection.insert_many(records)
            total_inserted += len(result.inserted_ids)
            print(f"✅ Добавлено {len(result.inserted_ids)} записей из {file_name}")
        else:
            print(f"⚠️ Нет данных для вставки ({file_name})")

    except Exception as e:
        print(f"❌ Ошибка при обработке {file_name}: {e}")

# === 4. Финальный отчёт ===
duration = datetime.now() - start_time
count = collection.count_documents({})

print("\n📊 === ИТОГ ===")
print(f"📁 Всего файлов обработано: {len(files)}")
print(f"🧾 Всего вставлено записей: {total_inserted}")
print(f"💾 Общее количество документов в коллекции: {count}")
print(f"⏱️ Время выполнения: {duration}")

# === 5. Показываем пример одной вставленной записи ===
sample_doc = collection.find_one(sort=[("_id", -1)])
if sample_doc:
    sample_doc.pop("_id", None)
    for k, v in sample_doc.items():
        if isinstance(v, pd.Timestamp):
            sample_doc[k] = v.strftime("%Y-%m-%d %H:%M:%S")

    print("\n🕓 Пример последней вставленной записи:")
    print(json.dumps(sample_doc, ensure_ascii=False, indent=2))