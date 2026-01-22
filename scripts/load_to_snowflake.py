import snowflake.connector
import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# Configuration (Env Vars should be set)
USER = os.getenv('SNOWFLAKE_USER')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SCHEMA = 'PUBLIC'

DATA_DIR = "data/raw"

def get_connection():
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )

def upload_files():
    print("Connecting to Snowflake...")
    try:
        conn = get_connection()
        print("Connected.")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    for filename in files:
        table_name = filename.replace('.csv', '').upper()
        file_path = os.path.join(DATA_DIR, filename)
        
        print(f"Processing {filename} -> {table_name}...")
        
        # Read locally
        df = pd.read_csv(file_path)
        
        # Standardization: Snowflake likes UPPERCASE columns
        df.columns = [c.upper() for c in df.columns]
        
        # Write to Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            df, 
            table_name, 
            auto_create_table=True, # Create table if not exists based on DF schema
            overwrite=True          # For dev, we overwrite
        )
        
        if success:
            print(f"Success! Uploaded {nrows} rows to {table_name}.")
        else:
            print(f"Failed to upload {table_name}")
            
    conn.close()

if __name__ == "__main__":
    if not all([USER, PASSWORD, ACCOUNT, DATABASE]):
        print("ERROR: Please set SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE env vars.")
    else:
        upload_files()
