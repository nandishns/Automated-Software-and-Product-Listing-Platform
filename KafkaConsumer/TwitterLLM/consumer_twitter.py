import requests
import json
from dotenv import load_dotenv
from os import getenv
from pymongo import MongoClient
from datetime import datetime
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer

# Load environment variables
load_dotenv()
API_TOKEN = getenv("G2_API_KEY")
MONGO_CONN_STRING = getenv("MONGO_CONN_STRING")
GOOGLE_TOKEN = getenv("GOOGLE_API_KEY")

# Configure Google Generative AI
genai.configure(api_key=GOOGLE_TOKEN)

# MongoDB setup
mongo_client = MongoClient(MONGO_CONN_STRING)
db = mongo_client.g2hack
products_collection = db.unavailableProducts

def check_mongo_connection():
    """ Checks MongoDB connection. """
    try:
        mongo_client.server_info()
        print("Connected to MongoDB")
    except Exception as e:
        print("Failed to connect to MongoDB:", e)

def fetch_products_g2(api_token, filter_name=None):
    """ Fetches products from G2 using the provided filter name. """
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

def fetch_products_from_google(message_data):
    """ Fetches product information using Google's generative AI. """
    print("Fetching products from Google")
    model = genai.GenerativeModel('gemini-pro')
    prompt = f"""{message_data} \n Imagine a digital assistant meticulously analyzing a diverse collection
      of announcements related to the launch of new products and services in various industries.
        This assistant is tasked with identifying and categorizing each product or service mentioned, 
        discerning whether each one represents a fresh market entry or an update to an existing offering.
          The goal is to compile this information into a straightforward, accessible format. Specifically,
            the assistant is required to present its findings as a list, focusing solely on the names of these 
            products or services, neatly organized into an array. The array should exclusively contain the names, 
            clearly distinguishing between novel introductions and updates to pre-existing entities, thus providing a clear,
              concise overview of the recent developments highlighted in the announcements, identify if the product is B2B product. 
              Make sure that the product you identify is a B2B product and only then include it in the list.
                Give the output in a json format which gives the product name and the status of the same whether
                  its a new product or just a update to the existing product. The status should either be New Product 
                  or Update to existing product.Keep the key name of the product name 
                  as Product Name and the status as Status """
    response = model.generate_content(prompt)
    # print(response.text)
    return response.text

def process_product_info(message_data):
    """ Processes each message to extract and handle product information. """
    print("Processing message")
    response_text = fetch_products_from_google(message_data)
    if response_text:
        clean_json = response_text.lstrip("```json").lstrip("```JSON").rstrip("```").strip()
        # print(clean_json, "Cleaned JSON")
        try:
            products = json.loads(clean_json)
            print("Products:", products)
            for product in products:
                process_individual_product(product)
        except json.JSONDecodeError as e:
            # print(f"Failed to decode JSON: {str(e)}")
            pass

def process_individual_product(product):
    """ Processes each individual product entry. """
    status = product.get("Status")
    product_name = product.get('Product Name')
    if status == "New Product":
        print(f"Processing new product: {product_name}")
        handle_new_product(product_name)

def handle_new_product(product_name):
    """ Checks if new product exists in G2 and handles storage if not found. """
    g2_response = fetch_products_g2(API_TOKEN, filter_name=product_name)
    if g2_response and not g2_response.get('data'):
        print(f"Product not found in G2: {product_name}")
        document = {
            "product_name": product_name,
            "timestamp": datetime.now()
        }
        products_collection.insert_one(document)
    else:
        print(f"Product found in G2: {product_name}")

def main():
    """ Main function to setup Kafka consumer and process messages. """
    check_mongo_connection()
    print("Setting up Kafka consumer")
    consumer = KafkaConsumer(
        'x-llm',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        for message in consumer:
            # print(f"Received message: {message.value.decode('utf-8')}")
            executor.submit(process_product_info, message.value.decode('utf-8'))

if __name__ == "__main__":
    main()
