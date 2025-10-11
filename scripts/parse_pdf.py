import pdfplumber
import glob
from pymongo import MongoClient
from datetime import datetime

# --- Подключение ---
client = MongoClient("mongodb://ds_user:StrongPassword123@185.22.67.9:27017/yoyoflot?authSource=yoyoflot")
db = client["yoyoflot"]
collection = db["timetable"]


# --- Пути ко всем частям PDF ---
PDF_PARTS = sorted(glob.glob("skyteampdfchunks/Skyteam_Timetable_part*.pdf"))
print(f"📦 Найдено {len(PDF_PARTS)} частей PDF")

LOG_FILE = "parse_log.txt"

def log(msg):
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

def parse_route_block(block, direction):
    if not block or len(block) < 8:
        return None
    try:
        return {
            "direction": direction,
            "validity": block[0],
            "days": block[2],
            "dep_time": block[3],
            "arr_time": block[4],
            "flight": block[5],
            "aircraft": block[6],
            "travel_time": block[8],
        }
    except Exception:
        return None

def parse_page(page_table, last_headers):
    routes = []
    from_to_left = last_headers.get("from_left")
    from_to_right = last_headers.get("from_right")
    to_left = last_headers.get("to_left")
    to_right = last_headers.get("to_right")

    # обновляем заголовки, если они есть
    for row in page_table:
        if row and row[0] == "FROM:":
            try:
                from_to_left = (row[1], row[7])
                from_to_right = (row[12], row[18])
            except Exception:
                pass
        if row and row[0] == "TO:":
            try:
                to_left = (row[1], row[7])
                to_right = (row[12], row[18])
            except Exception:
                pass

    # если нет новых — используем старые
    if from_to_left and to_left:
        last_headers.update({
            "from_left": from_to_left,
            "from_right": from_to_right,
            "to_left": to_left,
            "to_right": to_right,
        })
    else:
        log("⚠️ Нет заголовков FROM/TO — используем предыдущие")

    if not last_headers["from_left"] or not last_headers["to_left"]:
        log("🚫 Заголовков нет вообще — пропуск страницы")
        return [], last_headers

    for row in page_table[3:]:
        left = parse_route_block(row[:9], "L")
        if left:
            left["from"] = {"city": last_headers["from_left"][0], "code": last_headers["from_left"][1]}
            left["to"] = {"city": last_headers["to_left"][0], "code": last_headers["to_left"][1]}
            routes.append(left)
        right = parse_route_block(row[10:], "R")
        if right and last_headers["from_right"] and last_headers["to_right"]:
            right["from"] = {"city": last_headers["from_right"][0], "code": last_headers["from_right"][1]}
            right["to"] = {"city": last_headers["to_right"][0], "code": last_headers["to_right"][1]}
            routes.append(right)

    return routes, last_headers

total_inserted = 0
last_headers = {"from_left": None, "from_right": None, "to_left": None, "to_right": None}

for pdf_path in PDF_PARTS:
    log(f"\n📘 Обрабатывается файл: {pdf_path}")
    try:
        with pdfplumber.open(pdf_path) as pdf:
            file_docs = []
            for p_i, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                if not tables:
                    continue
                for t in tables:
                    parsed, last_headers = parse_page(t, last_headers)
                    if parsed:
                        file_docs.extend(parsed)
            if file_docs:
                collection.insert_many(file_docs)
                total_inserted += len(file_docs)
                log(f"✅ {pdf_path}: добавлено {len(file_docs)} рейсов (всего {total_inserted})")
            else:
                log(f"⚠️ {pdf_path}: нет данных")
    except Exception as e:
        log(f"❌ Ошибка при обработке {pdf_path}: {e}")

log(f"\n🎯 Итог: загружено {total_inserted} рейсов в MongoDB.")
print(f"\n🎯 Загружено {total_inserted} рейсов в MongoDB. См. {LOG_FILE}")
