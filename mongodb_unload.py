import os
from datetime import datetime
import time

import pandas as pd
import yaml
from bson import ObjectId
from pymongo import MongoClient


with open("settings.yml", "r") as yml:
    settings = yaml.load(yml, Loader=yaml.SafeLoader)

env_type = os.environ.get("ENV_TYPE")

db_user = settings[env_type]["mongodb"]["username"]
db_password = settings[env_type]["mongodb"]["password"]
db_host = settings[env_type]["mongodb"]["host"]

if env_type == "test" or env_type == "local":
    # host = settings[env_type]["db"]["host"]
    # client = MongoClient(f"mongodb://{host}")
    client_str = (
        f"mongodb+srv://{db_user}:{db_password}@{db_host}/"
        + "?retryWrites=true&w=majority"
    )
    client = MongoClient(client_str)
else:
    client_str = (
        f"mongodb+srv://{db_user}:{db_password}@{db_host}/"
        + "?ssl=true&ssl_cert_reqs=CERT_NONE"
    )
    client = MongoClient(client_str)

result = client.S2M.students.find()
for doc in result:
    print(doc)

user_data = pd.DataFrame()
print(user_data.head(5))

# df_student_merged = pd.read_csv(
#     "./migration/source_tmp/student_merged.csv",
#     dtype=str,
#     encoding="utf8",
# )

# df_student_merged = df_student_merged.fillna("")

# for index, row in df_student_merged.iterrows():

#     # _id = (
#     #     ObjectId(
#     #         str(
#     #             int(
#     #                 dateutil.parser.parse(
#     #                     f"{str(row.created_at).replace(' ','T')}.000Z"
#     #                 ).timestamp()
#     #             )
#     #         ).ljust(24, "0")
#     #     )
#     #     if row.created_at
#     #     else ObjectId(str(index).zfill(24))
#     # )

#     _id = str(index).zfill(24)

#     student_dict = {
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

#     print(_id)
#     client.S2M.students.find_one_and_update(
#         {"_id": ObjectId(_id)}, {"$set": student_dict}, upsert=True,
#     )
#     df_student_merged.at[index, "id_s2m"] = _id

# df_student_merged.to_csv("./migration/source_tmp/student_merged.csv")

# if row.created_at:
#     print(ObjectId(_id))
#     print(
#         date_time_milliseconds(
#             datetime.strptime(row.created_at, "%Y-%m-%d %H:%M:%S")
#         )
#     )
#     print(
#         hex(
#             int(
#                 time.mktime(
#                     time.strptime(row.created_at, "%Y-%m-%d %H:%M:%S")
#                 )
#             )
#         )
#     )