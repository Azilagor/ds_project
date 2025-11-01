Serafim, [01.11.2025 01:49]
import os
import pandas as pd
import json
from datetime import datetime
from pathlib import Path

# === 1. Настройки ===
INPUT_FOLDER = "./Airlines/YourBoardingPassDotAero"
OUTPUT_FOLDER = "./Processed"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

print("🚀 Полный конвейер: Excel → JSON (в памяти) → карточки пассажиров\n")

start_time = datetime.now()

# === 2. Проверяем наличие Excel-файлов ===
files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.endswith(".xlsx")])
if not files:
    print("❌ В папке нет Excel-файлов!")
    exit()

print(f"📂 Найдено файлов: {len(files)}\n")

# === 3. Обработка каждого файла ===
for idx, file_name in enumerate(files, start=1):
    file_path = os.path.join(INPUT_FOLDER, file_name)
    output_clean = os.path.join(OUTPUT_FOLDER, file_name.replace(".xlsx", "_c.json"))

    print(f"[{idx}/{len(files)}] 🔄 Обработка: {file_name}")

    try:
        # === 1️⃣ Чтение Excel ===
        all_sheets = pd.read_excel(file_path, sheet_name=None, dtype=str, na_filter=False)
        df = pd.concat(all_sheets.values(), ignore_index=True)

        # Нормализация данных и защита от потери N/A
        df.columns = df.columns.astype(str)
        df = df.fillna("")
        df = df.replace({"n/a": "N/A", "na": "N/A", "NaN": "N/A", "NA": "N/A"}, regex=True)
        df["source_file"] = file_name

        # === 2️⃣ Преобразуем в JSON (в памяти) ===
        raw_records = df.to_dict(orient="records")
        print(f"📄 Прочитано {len(raw_records)} строк из {file_name}")

        # === 3️⃣ Преобразуем в карточки ===
        passengers = []
        current = {}

        for row in raw_records:
            bp = (row.get("BOARDING PASS") or "").strip()
            name = (row.get("Unnamed: 1") or "").strip()
            col2 = (row.get("Unnamed: 2") or "").strip()
            col3 = (row.get("Unnamed: 3") or "").strip()
            col4 = (row.get("Unnamed: 4") or "").strip()
            seq_val = (row.get("SEQUENCE:") or "").strip()
            col6 = (row.get("Unnamed: 6") or "").strip()
            src = (row.get("source_file") or "").strip()

            vals = {str(i): (row.get(str(i)) or "").strip() for i in range(0, 200)}

            # === Начало новой карточки ===
            if bp in ("MR", "MRS"):
                if current:
                    passengers.append(current)
                current = {
                    "Title": bp,
                    "Passenger": name or None,
                    "SeatClass": None,
                    "Sequence": None,
                    "SourceFile": src or None
                }
                for col, val in vals.items():
                    if val == "Y":
                        current["SeatClass"] = "Y"
                        current["Sequence"] = col
                        break

            # === Информация о рейсе ===
            elif bp.startswith("SU"):
                current["Flight"] = bp
                current["From"] = col3 or None
                for col in ("32", "50", "77", "87"):
                    val = vals.get(col, "")
                    if val and val not in ("Y", "N/A"):
                        current["To"] = val
                        break

            # === Коды аэропортов ===
            elif bp == "GATE":
                current["FromCode"] = col3 or None
                current["Arrow"] = seq_val or "->"
                for col in ("32", "50", "77", "87"):
                    val = vals.get(col, "")
                    if val not in ("", "N/A"):
                        current["ToCode"] = val
                        break

            # === Дата и время ===
            elif bp[:4].isdigit() and "-" in bp:
                current["Date"] = bp
                current["Time"] = col2 or None
                current["Operator"] = col4 or None

            # === Примечание и место ===
            elif bp.startswith("Boarding"):
                current["Note"] = bp
                current["SeatLabel"] = col6 or "SEAT"

Serafim, [01.11.2025 01:49]
# Добавляем Seat (N/A или номер)
                for val in vals.values():
                    if val == "N/A":
                        current["Seat"] = "N/A"
                        break
                    elif len(val) in (2, 3, 4) and val[0].isdigit():  # например, 23A, 12F
                        current["Seat"] = val
                        break

            # === Бронирование (PNR) ===
            elif bp.startswith("PNR"):
                current["PNR"] = name or None
                current["TicketType"] = col3 or None
                current["TicketNumber"] = col4 or None

        if current:
            passengers.append(current)

        # === 4️⃣ Очистка пустых полей ===
        for p in passengers:
            for k in list(p.keys()):
                if p[k] in ("", None):
                    del p[k]

        # === 5️⃣ Сохраняем только clean JSON ===
        Path(output_clean).write_text(
            json.dumps(passengers, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        print(f"✅ Готово: {os.path.basename(output_clean)} ({len(passengers)} карточек)\n")

    except Exception as e:
        print(f"❌ Ошибка при обработке {file_name}: {e}\n")

# === 6️⃣ Итог ===
duration = datetime.now() - start_time
print("\n📊 Итог:")
print(f"Всего файлов обработано: {len(files)}")
print(f"⏱️ Время выполнения: {duration}")
print(f"📁 Все результаты сохранены в: {OUTPUT_FOLDER}")
print("✅ Готово! Все файлы успешно обработаны.")