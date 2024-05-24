import pandas as pd
from snowflake.connector import connect
from dotenv import load_dotenv
import os

# Load environment variables from .env file
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


def load_csv_to_df(file_path, columns):
    return pd.read_csv(file_path, names=columns)


def preprocess_df(df, column_types):
    for column, col_type in column_types.items():
        if col_type == "int":
            df[column] = df[column].fillna(0).astype(int)
        else:
            df[column] = df[column].fillna("")
    return df


def insert_data_in_batches(df, table_name, key_columns, batch_size=1000):
    cursor = conn.cursor()
    for start in range(0, len(df), batch_size):
        end = min(start + batch_size, len(df))
        batch_df = df.iloc[start:end]
        values = [tuple(row) for row in batch_df.to_numpy()]
        
        for value in values:
            merge_query = f"""
                MERGE INTO {table_name} AS target
                USING (SELECT {', '.join(['%s AS ' + col for col in df.columns])}) AS source
                ON {" AND ".join([f"target.{col} = source.{col}" for col in key_columns])}
                WHEN MATCHED THEN UPDATE SET {", ".join([f"target.{col} = source.{col}" for col in df.columns if col not in key_columns])}
                WHEN NOT MATCHED THEN INSERT ({', '.join(df.columns)}) VALUES ({', '.join(['source.' + col for col in df.columns])});
            """
            cursor.execute(merge_query, value)
            
    cursor.close()


# Define column names and types for each table
tables = {
    "employees": {
        "file": "../data_challenge_files/hired_employees.csv",
        "columns": ["id", "name", "datetime", "department_id", "job_id"],
        "types": {
            "id": "int",
            "name": "str",
            "datetime": "str",
            "department_id": "int",
            "job_id": "int",
        },
        "key_columns": ["id"]
    },
    "departments": {
        "file": "../data_challenge_files/departments.csv",
        "columns": ["id", "department"],
        "types": {"id": "int", "department": "str"},
        "key_columns": ["id"]
    },
    "jobs": {
        "file": "../data_challenge_files/jobs.csv",
        "columns": ["id", "job"],
        "types": {"id": "int", "job": "str"},
        "key_columns": ["id"]
    },
}

# Load, preprocess, and insert data for each table
for table, details in tables.items():
    csv_path = os.path.join(os.path.dirname(__file__), details["file"])
    df = load_csv_to_df(csv_path, details["columns"])
    df = preprocess_df(df, details["types"])
    insert_data_in_batches(df, table, details["key_columns"])

# Commit and close the connection
conn.commit()
conn.close()
print("Data loaded successfully!")
