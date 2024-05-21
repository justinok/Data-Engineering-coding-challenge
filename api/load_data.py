import pandas as pd
from snowflake.connector import connect
from dotenv import load_dotenv
import os


load_dotenv()

# Snowflake connection details
try:
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    print("Connection to Snowflake successful!")
except Exception as e:
    print(f"Error connecting to Snowflake: {e}")

# Define column names for this table as test
columns = ["id", "name", "datetime", "department_id", "job_id"]


csv_path = os.path.join(
    os.path.dirname(__file__), "../data_challenge_files/hired_employees.csv"
)
employees_df = pd.read_csv(csv_path, names=columns)

# Preprocess DataFrame to handle missing values
employees_df["id"] = employees_df["id"].fillna(0).astype(int)
employees_df["name"] = employees_df["name"].fillna("")
employees_df["datetime"] = employees_df["datetime"].fillna("")
employees_df["department_id"] = employees_df[
    "department_id"].fillna(0).astype(int)
employees_df["job_id"] = employees_df["job_id"].fillna(0).astype(int)


# Function to insert data in batches
def insert_data_in_batches(df, table_name, batch_size=1000):
    cursor = conn.cursor()
    for start in range(0, len(df), batch_size):
        end = min(start + batch_size, len(df))
        batch_df = df.iloc[start:end]
        values = [tuple(row) for row in batch_df.to_numpy()]
        cursor.executemany(
            f"""
            INSERT INTO {table_name}
            (id, name, datetime, department_id, job_id)
            VALUES (%s, %s, %s, %s, %s)""",
            values,
        )
    cursor.close()


# Insert data in batches if connection is successful
if conn:
    insert_data_in_batches(employees_df, "employees")
    conn.commit()
    conn.close()
    print("Data loaded successfully!")
else:
    print("Connection to Snowflake failed.")
