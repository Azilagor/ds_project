from pymongo import MongoClient


client = MongoClient(
    host="",
    port=27017,
    username="yoyoadmin",
    password="Y,
    authSource="yoyoflot",)

db = client["yoyoflot"]

cleaned = []

for user in db["forum_profiles"].find():
    nickname = user.get("NickName")
    real_name = user.get("Real Name", {})
    first = real_name.get("First Name") if real_name else None
    last = real_name.get("Last Name") if real_name else None
    sex = user.get("Sex")
    
    # Программы лояльности
    for lp in user.get("Loyality Programm", []):
        lp_program = lp.get("programm")
        lp_number = lp.get("Number", "").strip()
        lp_status = lp.get("Status")

        # Зарегистрированные рейсы
        for flight in user.get("Registered Flights", []):
            cleaned.append({
                "nickname": nickname,
                "first_name": first,
                "last_name": last,
                "sex": sex,
                "loyalty_program": lp_program,
                "loyalty_number": lp_number,
                "loyalty_status": lp_status,
                "date": flight.get("Date"),
                "flight": flight.get("Flight"),
                "codeshare": flight.get("Codeshare"),
                "from_city": flight.get("Departure", {}).get("City"),
                "from_airport": flight.get("Departure", {}).get("Airport"),
                "to_city": flight.get("Arrival", {}).get("City"),
                "to_airport": flight.get("Arrival", {}).get("Airport"),
            })

db["forum_profiles_clean"].drop()
if cleaned:
    db["forum_profiles_clean"].insert_many(cleaned)

print(f"[+] Normalized {len(cleaned)} records to forum_profiles_clean")
