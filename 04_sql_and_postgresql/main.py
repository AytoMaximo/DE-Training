import psycopg2
from clickhouse_driver import Client

from pymongo import MongoClient
from pprint import pprint
import json

# PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",
    port=7432,
    user="user",
    password="password",
    dbname="example_db"
)
pg_cursor = pg_conn.cursor()
pg_cursor.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT);")
pg_cursor.execute("INSERT INTO users(name) VALUES (%s)", ("Alice",))
pg_cursor.execute("SELECT * FROM users;")
print("PostgreSQL:", pg_cursor.fetchall())
pg_conn.commit()
pg_cursor.close()
pg_conn.close()

# ClickHouse
ch_client = Client(host='localhost')
ch_client.execute("CREATE TABLE IF NOT EXISTS test (id UInt32, name String) ENGINE = MergeTree() ORDER BY id")
ch_client.execute("INSERT INTO test (id, name) VALUES", [(1, 'Bob')])
rows = ch_client.execute("SELECT * FROM test")
print("ClickHouse:", rows)

# MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")

# mongo_db = mongo_client["test_db"]
# mongo_collection = mongo_db["users"]
# mongo_collection.insert_one({"name": "Charlie"})
# print("MongoDB:", list(mongo_collection.find({}, {"_id": 0})))

db = mongo_client["alcomarket"]
products = db["products"]

# 2. Очистка коллекции перед загрузкой (для повторного запуска)
products.drop()

# 3. Загрузка данных из файла products.json
with open("products.json", "r") as f:
    data = json.load(f)
    products.insert_many(data)
print("\n📦 Все товары:")
for doc in products.find():
    pprint(doc)

# Пример полезных команд
print("\n🍷 Вина с рейтингом сомелье > 4.5:")
for doc in products.find({"type": "wine", "rating.sommelier": {"$gt": 4.5}}):
    pprint(doc)

print("\n📑 Только имя и цена всех товаров:")
for doc in products.find({}, {"name": 1, "price": 1, "_id": 0}):
    pprint(doc)

print("\n🌍 Уникальные страны происхождения:")
pprint(products.distinct("country"))

print("\n📊 Средняя цена пива по странам:")
pipeline = [
    {"$match": {"type": "beer"}},
    {"$group": {"_id": "$country", "avgPrice": {"$avg": "$price"}}},
    {"$sort": {"avgPrice": -1}}
]
for doc in products.aggregate(pipeline):
    pprint(doc)

print("\n🔄 Обновим цену Guinness до 2.99:")
products.update_one({"name": "Guinness Draught"}, {"$set": {"price": 2.99}})
pprint(products.find_one({"name": "Guinness Draught"}))

print("\n➕ Добавим поле stock = 100 ко всем товарам:")
products.update_many({}, {"$set": {"stock": 100}})
pprint(products.find_one())

print("\n❌ Удалим товары, которые недоступны (available: false):")
result = products.delete_many({"available": False})
print(f"Удалено документов: {result.deleted_count}")

print("\n📦 Все товары после удаления:")
for doc in products.find():
    pprint(doc)

print("\n📚 Индексы коллекции:")
pprint(products.index_information())

# Создание индекса
products.create_index("type")

print("\n🧾 Индексы после создания индекса по type:")
pprint(products.index_information())