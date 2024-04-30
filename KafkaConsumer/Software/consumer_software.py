from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import requests
import json
from dotenv import load_dotenv
from os import getenv
from pymongo import MongoClient
from datetime import datetime


load_dotenv()
api_token = getenv("G2_API_KEY")
mongo_conn_string = getenv("MONGO_CONN_STRING")

# Setup MongoDB connection
client = MongoClient(mongo_conn_string)
db = client.g2hack
unavailable_products_collection = db.unavailableProducts

def ping_mongo():
    try:
        client.server_info()
        print("Connected to MongoDB")
    except:
        print("Failed to connect to MongoDB")

def list_products(api_token, filter_name=None):
    url = "https://data.g2.com/api/v1/products"
    headers = {
        "Authorization": f"Token token={api_token}",
        "Content-Type": "application/vnd.api+json"
    }
    params = {'filter[name]': filter_name} if filter_name else {}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch products, status code: {response.status_code}")
        return None

def process_message(message_data):
    product_name = message_data.get('name')
    if product_name:
        g2_response = list_products(api_token, filter_name=product_name)
        if g2_response and not g2_response.get('data'):
            print(f"Product not found in G2: {product_name}")
            document = {
                "product_name": product_name,
                "timestamp": datetime.now(),
                "desc": message_data.get('description'),
            }
            unavailable_products_collection.insert_one(document)
        else:
            print(f"Product found in G2: {product_name}")

def main():
    ping_mongo()
    print("Setting up Kafka consumer")
    consumer = KafkaConsumer(
        'Software',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    with ThreadPoolExecutor(max_workers=10) as executor:
        for message in consumer:
            data = message.value
            executor.submit(process_message, data)

if __name__ == "__main__":
    main()
