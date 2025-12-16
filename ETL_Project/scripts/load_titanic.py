import os

import pandas as pd

from supabase import create_client, Client

from dotenv import load_dotenv
 

# Initialize Supabase Client


def get_supabase_client():

    load_dotenv()
 
    url = os.getenv("supabase_url")

    key = os.getenv("supabase_key")
 
    if not url or not key:

        raise ValueError("Missing Supabase URL or Supabase KEY in .env")
 
    return create_client(url, key)
 

# Create titanic_data table (if not exists)



def create_table_if_not_exists():

    try:

        supabase = get_supabase_client()
 
        create_table_sql = """

        CREATE TABLE IF NOT EXISTS titanic_data (
    id BIGSERIAL PRIMARY KEY,
    survived INTEGER,
    pclass INTEGER,
    name TEXT,
    sex TEXT,
    age FLOAT,
    sibsp INTEGER,
    parch INTEGER,
    ticket TEXT,
    fare FLOAT,
    cabin TEXT,
    embarked TEXT,
    familysize INTEGER,
    isalone INTEGER
    
);

        """
 
        try:

            supabase.rpc("execute_sql", {"query": create_table_sql}).execute()

            print("Table 'iris_data' created or already exists.")

        except Exception as e:

            print(f"RPC failed: {e}")

            print("Assuming table already exists or will auto-create.")
 
    except Exception as e:

        print(f"Error creating table: {e}")

        print("Continuing with data insertion...")
 
 

# Load CSV into Supabase


def load_to_supabase(staged_path: str, table_name: str = "titanic_data"):


    if not os.path.isabs(staged_path):

        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))
 
    print(f"Looking for the data file at: {staged_path}")
 
    if not os.path.exists(staged_path):

        print(f"Error: File not found at {staged_path}")

        print("Run transform_titanic.py first.")

        return
 
    try:

        supabase = get_supabase_client()
 
        df = pd.read_csv(staged_path)

        total_rows = len(df)

        batch_size = 50
 
        print(f"Loading {total_rows} rows into table '{table_name}'...")
 
        # Insert in batches

        for i in range(0, total_rows, batch_size):

            batch = df.iloc[i: i + batch_size].copy()
 
            # Replace NaN with None (Supabase requires None for NULL)

            batch = batch.where(pd.notnull(batch), None)
 
            records = batch.to_dict("records")
 
            try:

                supabase.table(table_name).insert(records).execute()
 
                end = min(i + batch_size, total_rows)

                print(f"Inserted rows {i + 1} – {end} of {total_rows}")
 
            except Exception as e:

                print(f"Error in batch {i // batch_size + 1}: {str(e)}")

                continue
 
        print(f"Finished loading data into '{table_name}'.")
 
    except Exception as e:

        print(f"Error loading data: {e}")
 


if __name__ == "__main__":
 
    staged_csv_path = os.path.join("..", "data", "staged", "titanic_transformed.csv")
 
    # create_table_if_not_exists()

    load_to_supabase(staged_csv_path)