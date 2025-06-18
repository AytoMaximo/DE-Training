import os
import json
from datetime import datetime, timedelta
from pymongo import MongoClient

def archive_inactive_users():
    # Настройка подключения к MongoDB
    client = MongoClient()
    db = client['my_database']
    events = db.user_events
    archived = db.archived_users

    now = datetime.utcnow()
    reg_cutoff = now - timedelta(days=30)
    act_cutoff = now - timedelta(days=14)

    # Агрегация: для каждого пользователя находим дату регистрации и время последнего события
    pipeline = [
        {"$group": {
            "_id": "$user_id",
            "last_event": {"$max": "$event_time"},
            "registration_date": {"$first": "$user_info.registration_date"}
        }},
        {"$match": {
            "registration_date": {"$lt": reg_cutoff},
            "last_event": {"$lt": act_cutoff}
        }}
    ]

    inactive = list(events.aggregate(pipeline))
    user_ids = [doc["_id"] for doc in inactive]

    # Перемещение документов в архивную коллекцию
    if user_ids:
        to_archive = list(events.find({"user_id": {"$in": user_ids}}))
        archived.insert_many(to_archive)
        events.delete_many({"user_id": {"$in": user_ids}})

    # Подготовка отчёта
    report = {
        "date": now.strftime("%Y-%m-%d"),
        "archived_users_count": len(user_ids),
        "archived_user_ids": user_ids
    }

    # Сохранение отчёта
    filename = now.strftime("%Y-%m-%d") + ".json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=4)
    print(f"Archived {len(user_ids)} users. Report saved to {filename}")

if __name__ == "__main__":
    archive_inactive_users()