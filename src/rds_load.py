# rds
import os
import pymysql
import time
import pandas as pd
import yaml
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime


with open("settings.yml", "r") as yml:
    settings = yaml.load(yml, Loader=yaml.SafeLoader)

env_type = os.environ.get("ENV_TYPE")

db_user = settings[env_type]["rds"]["username"]
db_password = settings[env_type]["rds"]["password"]
db_host = settings[env_type]["rds"]["host"]
db_name = settings[env_type]["rds"]["db_name"]

con = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        db=db_name,
        cursorclass=pymysql.cursors.DictCursor
)

try:
    with con.cursor() as cur:
        sql = 'SELECT * FROM students'
        cur.execute(sql)
        result = cur.fetchall()
        print(result)
except:
    print('Error !!!!')

# student_dict = {
#         "_id": ObjectId(_id),
#         "email": row["email"],
#         "uid": "",
#         "address": row["address_s2m"],
#         "birth_date": datetime.strptime(row["birth_date_s2m"], "%Y-%m-%d")
#         if row["birth_date_s2m"]
#         else None,
#         "first_name": row["first_name_s2m"],
#         "first_name_kana": row["first_name_kana_s2m"],
#         "gender": row["gender_s2m"],
#         "last_name": "",
#         "last_name_kana": "",
#         "name_emergency": row["name_emergency_s2m"],
#         "postcode": "",
#         "subscription": None,
#         "tel_emergency": row["tel_emergency_s2m"],
#         "tel_mobile_phone": row["tel_mobile_phone_s2m"],
#         "company": {
#             "name": row["company_name_s2m"],
#             "address": row["company_address_s2m"],
#             "tel": row["company_tel_s2m"],
#         },
#     }