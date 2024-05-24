# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app


# Define environment variables
ENV SNOWFLAKE_USER=your_snowflake_user
ENV SNOWFLAKE_PASSWORD=your_snowflake_password
ENV SNOWFLAKE_ACCOUNT=your_snowflake_account
ENV SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
ENV SNOWFLAKE_DATABASE=your_snowflake_database
ENV SNOWFLAKE_SCHEMA=your_snowflake_schema


# Install gcc and other necessary build tools
RUN apt-get update && apt-get install -y gcc g++ make

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run app.py when the container launches
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "api.app:gunicorn_app"]
