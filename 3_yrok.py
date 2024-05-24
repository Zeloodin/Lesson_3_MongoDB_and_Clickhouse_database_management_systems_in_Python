# Установите MongoDB на локальной машине,
# а также зарегистрируйтесь в онлайн-сервисе.
# https://www.mongodb.com/
# https://www.mongodb.com/products/compass

# Загрузите данные который вы получили на
# предыдущем уроке путем скрейпинга сайта с помощью Buautiful Soup в
# MongoDB и создайте базу данных и коллекции для их хранения.

# Поэкспериментируйте с различными методами запросов.
# Зарегистрируйтесь в ClickHouse.
# Загрузите данные в ClickHouse и создайте таблицу для их хранения.

from pymongo import MongoClient
from pymongo.errors import *
from pprint import pprint
import json
import requests

response = requests.get("https://gbcdn.mrgcdn.ru/uploads/asset/5560965/attachment/357f7ccb20abaeedb8ccfda8e1045098.json")

with open("data.json", "wb") as f:
    f.write(response.content)

with open("data.json", "r") as f:
    jsfil = json.load(f)

# # Чужой код.
# # https://github.com/rimtimti/parsing_data/blob/66de5417d037469ed28ea738f9389b85decd7374/sem_3_mongodb/homework_3.py
# def chunk_data(data, chunk_size):
#     for i in range(0, len(data), chunk_size):
#         yield data[i : i + chunk_size]
#
# chunk_size = 5000
# jsfil_chunks = list(chunk_data(jsfil, chunk_size))


client = MongoClient(host="localhost", port=27017)
db = client["crashes"]
info = db.info

info.delete_many({})

with open("data.json", "r") as f:
    data = json.load(f)

# # Чужой код.
# for chunk in jsfil_chunks:
#     info.insert_many(chunk)

count_dublicated = 0
for feature in data["features"]:
    _id = fr"""{feature.get("properties").get("tamainid")}{feature.get("properties").get("lat2")}{feature.get("properties").get("lon2")}"""
    feature["_id"] = _id
    try:
        info.insert_one(feature)
    except:
        count_dublicated += 1
        print(feature)
print(count_dublicated)







