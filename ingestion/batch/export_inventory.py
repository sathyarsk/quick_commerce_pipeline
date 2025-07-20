import json
from pymongo import MongoClient

# Connecting to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Choosing database and collection
db = client["quick_commerce"]
collection = db["inventory"]

# Loading JSON data from file
with open("inventory.json") as file:
    data = json.load(file)

# Inserting data 
if isinstance(data, list):
    collection.insert_many(data)
else:
    collection.insert_one(data)

print("Data inserted successfully.")
