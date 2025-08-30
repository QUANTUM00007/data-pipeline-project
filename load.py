import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

def load_data_to_db(processed_file_path):
    """Connects to the DB and loads the processed data."""
    load_dotenv() # Load environment variables from .env file

    try:
        # 1. Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cur = conn.cursor()
        print("Database connection successful.")

        # 2. Read the processed data
        df = pd.read_csv(processed_file_path)

        # 3. Insert data into the products table
        # This is a simple but effective way for smaller datasets
        for index, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO products (id, title, price, description, category, image, rating_rate, rating_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING; 
                """,
                (row['id'], row['title'], row['price'], row['description'], row['category'], row['image'], row['rating_rate'], row['rating_count'])
            )

        conn.commit()
        print(f"{len(df)} records inserted/updated successfully.")

    except FileNotFoundError:
        print(f"Error: The file {processed_file_path} was not found.")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        # 4. Close the cursor and connection
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    PROCESSED_DATA_PATH = "data/processed/products.csv"
    load_data_to_db(PROCESSED_DATA_PATH)