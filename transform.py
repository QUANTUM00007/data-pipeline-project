import pandas as pd
import json
from pathlib import Path

def transfrom_data(raw_data_path, processed_data_path):
    """Reads raw data, transforms it, and saves it as a CSV."""
    try:
        # 1. Read the raw JSON data
        with open(raw_data_path, 'r') as f:
            data = json.load(f)

        # 2. Convert to a pandas DataFrame
        # This is a common starting point for data manipulation
        df = pd.DataFrame(data)
        print("Successfully loaded data into pandas DataFrame.")

        # --- DATA CLEANING & TRANSFORMATION ---

        # 3. Handle nested 'rating' data
        # The 'rating' column contains a dictionary. We'll flatten it.
        df['rating_rate'] = df['rating'].apply(lambda x: x['rate'])
        df['rating_count'] = df['rating'].apply(lambda x: x['count'])

        # 4. Select and rename columns for clarity
        # We create a cleaner, more organized table
        df_processed = df[[
            'id',
            'title',
            'price',
            'description',
            'category',
            'image',
            'rating_rate',
            'rating_count'
        ]]

        # 5. Ensure 'price' and 'rating_rate' are numeric types
        df_processed['price'] = pd.to_numeric(df_processed['price'])
        df_processed['rating_rate'] = pd.to_numeric(df_processed['rating_rate'])

        print("Data transformation complete.")

        # --- SAVING THE PROCESSED DATA ---

        # 6. Save the cleaned data to a new CSV file
        # Ensure the directory exists before saving
        Path(processed_data_path).parent.mkdir(parents=True, exist_ok=True)
        df_processed.to_csv(processed_data_path, index=False)

        print(f"Processed data saved to {processed_data_path}")

    except FileNotFoundError:
        print(f"Error: The file {raw_data_path} was not found")
    except Exception as e:
        print(f"An error occurred during transformation: {e}")

if __name__ == "__main__":
    RAW_DATA_PATH = "data/raw/products.json"
    PROCESSED_DATA_PATH = "data/processed/products.csv"

    transfrom_data(RAW_DATA_PATH, PROCESSED_DATA_PATH)
    