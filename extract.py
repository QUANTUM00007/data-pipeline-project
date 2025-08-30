import requests
import json
from pathlib import Path

def fetch_data_from_api(api_url):
    """Fetches data from the given API URL."""
    try:
        # Send a GET request to the API
        response = requests.get(api_url)

        # Raise an exception if the request was unsuccessful (e.g., 404, 500)
        response.raise_for_status() 

        print(f"Successfully fetched data from {api_url}")
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def save_data_to_json(data, file_path):
    """Saves the given data to a JSON file."""
    if data is None:
        print("No data to save.")
        return

    try:
        # Ensure the directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        # Write data to the file
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Data successfully saved to {file_path}")

    except IOError as e:
        print(f"Error saving data to file: {e}")

# This block runs when the script is executed directly
if __name__ == "__main__":
    PRODUCTS_API_URL = "https://fakestoreapi.com/products"
    RAW_DATA_PATH = "data/raw/products.json"

    # 1. Fetch the data
    products_data = fetch_data_from_api(PRODUCTS_API_URL)

    # 2. Save the raw data
    save_data_to_json(products_data, RAW_DATA_PATH)