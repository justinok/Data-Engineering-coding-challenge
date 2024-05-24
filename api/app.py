from flask import Flask, jsonify
from dotenv import load_dotenv
import os
from snowflake.connector import connect


app = Flask(__name__)
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


@app.route('/employees-hired-by-job-department-quarter', methods=['GET'])
def get_employees_hired_by_job_department_quarter():
    query = """
        SELECT department, job,
            SUM(CASE WHEN DATE_PART('quarter', COALESCE(TRY_TO_TIMESTAMP(datetime, 'YYYY-MM-DDThh24:mi:ssZ'), CAST(NULL AS TIMESTAMP_NTZ))) = 1 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN DATE_PART('quarter', COALESCE(TRY_TO_TIMESTAMP(datetime, 'YYYY-MM-DDThh24:mi:ssZ'), CAST(NULL AS TIMESTAMP_NTZ))) = 2 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN DATE_PART('quarter', COALESCE(TRY_TO_TIMESTAMP(datetime, 'YYYY-MM-DDThh24:mi:ssZ'), CAST(NULL AS TIMESTAMP_NTZ))) = 3 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN DATE_PART('quarter', COALESCE(TRY_TO_TIMESTAMP(datetime, 'YYYY-MM-DDThh24:mi:ssZ'), CAST(NULL AS TIMESTAMP_NTZ))) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        JOIN jobs j ON e.job_id = j.id
        WHERE DATE_PART('year', COALESCE(TRY_TO_TIMESTAMP(datetime, 'YYYY-MM-DDThh24:mi:ssZ'), CAST(NULL AS TIMESTAMP_NTZ))) = 2021
        GROUP BY department, job
        ORDER BY department, job;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()

    # Formatear los resultados en un formato tabular
    columns = ['department', 'job', 'Q1', 'Q2', 'Q3', 'Q4']
    table = [dict(zip(columns, row)) for row in data]

    return jsonify(table)

@app.route('/departments-hired-above-mean', methods=['GET'])
def get_departments_hired_above_mean():
    query = """
        WITH mean_hired AS (
            SELECT AVG(count_hired) AS mean_hired
            FROM (
                SELECT d.department, COUNT(*) AS count_hired
                FROM employees e
                JOIN departments d ON e.department_id = d.id
                WHERE DATE_PART('year', TRY_TO_TIMESTAMP(e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 2021
                GROUP BY d.department
            )
        )
        SELECT d.id, d.department, COUNT(*) AS hired
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE DATE_PART('year', TRY_TO_TIMESTAMP(e.datetime, 'YYYY-MM-DDThh24:mi:ssZ')) = 2021
        GROUP BY d.id, d.department
        HAVING COUNT(*) > (SELECT mean_hired FROM mean_hired)
        ORDER BY hired DESC;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()

    # Formatear los resultados en una lista de diccionarios
    columns = ['id', 'department', 'hired']
    result = [dict(zip(columns, row)) for row in data]

    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
else:
    gunicorn_app = app