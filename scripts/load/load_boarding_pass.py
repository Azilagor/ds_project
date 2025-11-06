import pdfplumber
from pymongo import MongoClient

# --- Подключение к MongoDB ---
client = MongoClient("mongodb://ds_user:@:27017/yoyoflot?authSource=yoyoflot")
db = client["yoyoflot"]
collection = db["timetable"]

# --- Путь к PDF ---
PDF_PATH = "skyteampdfchunks/Skyteam_Timetable_part1.pdf"

# --- Парсер одной строки таблицы ---
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

# --- Парсер одной страницы ---
def parse_page(page_table, last_headers):
    routes = []
    from_to_left = last_headers.get("from_left")
    from_to_right = last_headers.get("from_right")
    to_left = last_headers.get("to_left")
    to_right = last_headers.get("to_right")

    # Проверяем наличие новых заголовков
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

    # Если заголовков нет — используем предыдущие
    if from_to_left and to_left:
        last_headers.update({
            "from_left": from_to_left,
            "from_right": from_to_right,
            "to_left": to_left,
            "to_right": to_right,
        })

    if not last_headers["from_left"] or not last_headers["to_left"]:
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


# --- Основной процесс ---
with pdfplumber.open(PDF_PATH) as pdf:
    docs = []
    last_headers = {"from_left": None, "from_right": None, "to_left": None, "to_right": None}

    for i, page in enumerate(pdf.pages[4:], start=5):  # со страницы 5
        tables = page.extract_tables()
        if not tables:
            continue

        for t in tables:
            parsed, last_headers = parse_page(t, last_headers)
            if parsed:
                print(f"[+] Страница {i}: найдено {len(parsed)} рейсов")
                docs.extend(parsed)

# --- Запись в MongoDB ---
if docs:
    collection.insert_many(docs)
    print(f" Загружено {len(docs)} рейсов в timetable (part1)")
else:
    print("Ничего не найдено.")
