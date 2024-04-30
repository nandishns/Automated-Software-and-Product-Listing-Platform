from flask import Flask, jsonify, request
import datetime
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
mongo_url = os.getenv("MONGO_CONN_STRING")

# Set up MongoDB connection
client = MongoClient(mongo_url)
db = client['g2hack']
collection = db['unavailableProducts']

app = Flask(__name__)

@app.route('/products', methods=['GET'])
def get_products():
    # Extract date from query parameters, use current date if not provided
    input_date_str = request.args.get('date', default=datetime.date.today().isoformat())
    input_date = datetime.datetime.strptime(input_date_str, '%Y-%m-%d').date()
    
    start_of_day = datetime.datetime(input_date.year, input_date.month, input_date.day)
    end_of_day = start_of_day + datetime.timedelta(days=1)

    # MongoDB query to fetch products
    query = {
        "timestamp": {
            "$gte": start_of_day,
            "$lt": end_of_day
        }
    }
    results = collection.find(query).sort("product_name", -1)

    # Processing results
    unique_products = {}
    for result in results:
        product_name = result["product_name"]
        if product_name not in unique_products:
            unique_products[product_name] = result.get("desc", "No description available")

    products = [{"Product Name": name, "Description": desc} for name, desc in unique_products.items()]
    return jsonify(products)

if __name__ == "__main__":
    app.run(debug=True)
