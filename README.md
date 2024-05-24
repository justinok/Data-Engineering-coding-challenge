# API for Uploading and Processing CSV Files

This project provides a local API to upload CSV files, process them, and load them into a Snowflake database. The API is built with Flask and can receive up to 3 CSV files via a POST request.

## Prerequisites

Make sure you have the following prerequisites installed in your environment:

- Python 3.x
- Flask
- pandas
- python-dotenv
- snowflake-connector-python
- asgiref
- werkzeug

## Installation

1. Clone this repository to your local machine:

```bash
git clone <REPOSITORY_URL>
cd <DIRECTORY_NAME>
```
2. Create and activate a virtual environment (optional but recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```
3. Install the required dependencies:
```bash
pip install -r requirements.txt
```
4. Create a .env file in the root directory of the project and add your Snowflake credentials:

```
SNOWFLAKE_USER=<your_user>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ACCOUNT=<your_account>
SNOWFLAKE_WAREHOUSE=<your_warehouse>
SNOWFLAKE_DATABASE=<your_database>
SNOWFLAKE_SCHEMA=<your_schema>
```

5. Run the api

```bash
gunicorn asgi_app:gunicorn_app
```
You can upload CSV files via a POST request to http://127.0.0.1:8000/upload-csv. The request should include the files with the key files[]