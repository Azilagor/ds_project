import pandas as pd
from pymongo import MongoClient

# --- 1. Загружаем все листы ---
all_sheets = pd.read_excel("../Airlines/YourBoardingPassDotAero/YourBoardingPassDotAero-2017-01-01.xlsx", sheet_name=None)

# --- 2. Объединяем их в один DataFrame ---
df = pd.concat(all_sheets.values(), ignore_index=True)

# --- 3. Чистим данные ---
df = df.dropna(axis=0, how="all")        # убрать пустые строки
df = df.dropna(axis=1, how="all")        # убрать пустые колонки
df = df.rename(columns=str)               # имена колонок в строки
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # удалить Unnamed
df = df.replace({pd.NA: None})            # заменить NaN -> None

# --- 4. Подключаемся к MongoDB ---
client = MongoClient("mongodb://ds_user:StrongPassword123@185.22.67.9:27017/yoyoflot?authSource=yoyoflot")
db = client["yoyoflot"]
collection = db["boarding_passes"]

# --- 5. Преобразуем и вставляем ---
data = df.to_dict(orient="records")

if data:
    collection.insert_many(data)
    print(f"✅ Загружено {len(data)} записей из {len(all_sheets)} листов в {db.name}.{collection.name}")
else:
    print("⚠️ Нет данных для вставки")