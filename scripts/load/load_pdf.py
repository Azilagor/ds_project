# import os
# import pdfplumber
# from pymongo import MongoClient

# client = MongoClient("mongodb://ds_user:StrongPassword123@185.22.67.9:27017/yoyoflot?authSource=yoyoflot")
# db = client["yoyoflot"]

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DATA_DIR = os.path.join(BASE_DIR, "Airlines")
# DEFAULT_PDF = os.path.join(DATA_DIR, "Skyteam_Timetable.pdf")

# def load_pdf_streaming(path, collection_name, batch_size=5):
#     col = db[collection_name]
#     batch = []
#     page_counter = 0

#     with pdfplumber.open(path) as pdf:
#         for i, page in enumerate(pdf.pages, start=1):
#             text = page.extract_text() or ""
#             batch.append({
#                 "page": i,
#                 "content": text
#             })
#             page_counter += 1

#             if len(batch) >= batch_size:
#                 col.insert_many(batch)
#                 print(f"[+] Inserted {len(batch)} pages (total: {page_counter})")
#                 batch.clear()

#     if batch:
#         col.insert_many(batch)
#         print(f"[+] Inserted last {len(batch)} pages (total: {page_counter})")

#     print(f"✅ Done. PDF pages inserted: {page_counter}, collection: {collection_name}")

# if __name__ == "__main__":
#     load_pdf_streaming(DEFAULT_PDF, "timetable", batch_size=5)



from PyPDF2 import PdfReader
import re

PDF_PATH = "Airlines/Skyteam_Timetable.pdf"
reader = PdfReader(PDF_PATH)
print(f"📄 Всего страниц: {len(reader.pages)}")

def parse_pdf_preview(path, max_pages=10):
    reader = PdfReader(path)
    pages = min(len(reader.pages), max_pages)
    print(f"📄 Всего страниц: {len(reader.pages)}. Покажем первые {pages}.")

    for i in range(pages):
        text = reader.pages[i].extract_text() or ""
        print(f"\n--- Страница {i+1} ---")
        print(text[:2000])  # печатаем первые 2000 символов, чтобы не перегрузить
        print("\n--- Конец страницы ---")

parse_pdf_preview(PDF_PATH, 10)
