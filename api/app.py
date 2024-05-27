from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os
import logging
from snowflake.connector import connect
from asgiref.wsgi import WsgiToAsgi
import pandas as pd
from werkzeug.utils import secure_filename

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
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

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "/tmp"

ALLOWED_FILES = {"hired_employees.csv", "departments.csv", "jobs.csv"}


def allowed_file(filename):
    return "." in filename and filename in ALLOWED_FILES


def load_csv_to_df(file_path, columns):
    return pd.read_csv(file_path, names=columns)


def query_to_dataframe(query):
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    df = pd.DataFrame(data, columns=columns)
    return df



def preprocess_df(df, column_types):
    for column, col_type in column_types.items():
        if col_type == "int":
            df[column] = df[column].fillna(0).astype(int)
        else:
            df[column] = df[column].fillna("")
    return df


def insert_data_in_batches(df, table_name, batch_size=1000):
    cursor = conn.cursor()
    for start in range(0, len(df), batch_size):
        end = min(start + batch_size, len(df))
        batch_df = df.iloc[start:end]
        values = [tuple(row) for row in batch_df.to_numpy()]
        cursor.executemany(
            f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})",
            values,
        )
    cursor.close()


@app.route("/upload-csv", methods=["POST"])
def upload_csv():
    if "files[]" not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    files = request.files.getlist("files[]")

    if len(files) > 3:
        return jsonify({"error": "Maximum 3 files are allowed"}), 400

    response = []
    for file in files:
        if file.filename == "":
            return jsonify({"error": "No file selected for uploading"}), 400

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(filepath)

            try:
                # Leer el archivo CSV
                df = pd.read_csv(filepath)
                first_line = df.iloc[0].to_dict()
                response.append(
                    {"filename": filename, "first_line": first_line})

                # Procesar y subir a Snowflake
                table_details = {
                    "hired_employees.csv": {
                        "table": "employees",
                        "columns": [
                            "id",
                            "name",
                            "datetime",
                            "department_id",
                            "job_id",
                        ],
                        "types": {
                            "id": "int",
                            "name": "str",
                            "datetime": "str",
                            "department_id": "int",
                            "job_id": "int",
                        },
                    },
                    "departments.csv": {
                        "table": "departments",
                        "columns": ["id", "department"],
                        "types": {"id": "int", "department": "str"},
                    },
                    "jobs.csv": {
                        "table": "jobs",
                        "columns": ["id", "job"],
                        "types": {"id": "int", "job": "str"},
                    },
                }

                table_name = table_details[filename]["table"]
                columns = table_details[filename]["columns"]
                column_types = table_details[filename]["types"]

                df = load_csv_to_df(filepath, columns)
                df = preprocess_df(df, column_types)
                query = f"SELECT * FROM {table_name}"
                df_snowflake = query_to_dataframe(query)
                df_to_insert = df[~df.isin(df_snowflake)].dropna()
                if not df_to_insert.empty:
                    insert_data_in_batches(df_to_insert, table_name)

            except Exception as e:
                logger.error(f"Error processing file {filename}: {e}")
                return jsonify(
                    {"error": f"Error processing file {filename}"}), 500
        else:
            return jsonify(
                {"error": f"File {file.filename} is not allowed"}), 400

    conn.commit()
    return jsonify(response), 200


@app.route("/employees-hired-by-job-department-quarter", methods=["GET"])
def get_employees_hired_by_job_department_quarter():
    query = """
        SELECT d.department, j.job,
            SUM(CASE WHEN DATE_PART('quarter', TRY_TO_TIMESTAMP(e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 1 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN DATE_PART('quarter', TRY_TO_TIMESTAMP(e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 2 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN DATE_PART('quarter', TRY_TO_TIMESTAMP(e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 3 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN DATE_PART('quarter', TRY_TO_TIMESTAMP(e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        JOIN jobs j ON e.job_id = j.id
        WHERE DATE_PART('year', TRY_TO_TIMESTAMP(e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 2021
        GROUP BY d.department, j.job
        ORDER BY d.department, j.job;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()

    columns = ["department", "job", "Q1", "Q2", "Q3", "Q4"]
    table = [dict(zip(columns, row)) for row in data]

    return jsonify(table)


@app.route("/departments-hired-above-mean", methods=["GET"])
def get_departments_hired_above_mean():
    query = """
        WITH mean_hired AS (
            SELECT AVG(count_hired) AS mean_hired
            FROM (
                SELECT d.department, COUNT(*) AS count_hired
                FROM employees e
                JOIN departments d ON e.department_id = d.id
                WHERE DATE_PART(
                    'year', TRY_TO_TIMESTAMP(
                        e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 2021
                GROUP BY d.department
            )
        )
        SELECT d.id, d.department, COUNT(*) AS hired
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE DATE_PART('year', TRY_TO_TIMESTAMP(
            e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 2021
        GROUP BY d.id, d.department
        HAVING COUNT(*) > (SELECT mean_hired FROM mean_hired)
        ORDER BY hired DESC;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()

    columns = ["id", "department", "hired"]
    result = [dict(zip(columns, row)) for row in data]

    return jsonify(result)


asgi_app = WsgiToAsgi(app)

if __name__ == "__main__":
    app.run(debug=True)
else:
    gunicorn_app = app
