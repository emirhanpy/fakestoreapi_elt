import requests
import json
import os

def extract_data():
    """Function to extract data from API"""
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code: {response.status_code}")

def load_data(data):
    """Function to load data into a JSON file"""
    file_path = os.path.join(os.getcwd(), 'fake_store_products.json')
    with open(file_path, 'w') as f:
        json.dump(data, f)
    return file_path



extracted_data = extract_data()
load_data_to_file = load_data(extracted_data)