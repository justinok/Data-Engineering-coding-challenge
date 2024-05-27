FROM python:3.11-slim

# Establecer el directorio de trabajo en /app
WORKDIR /app

# Copiar los archivos requirements.txt y el código fuente a la imagen
COPY requirements.txt ./
COPY . ./

# Install gcc and other necessary build tools
RUN apt-get update && apt-get install -y gcc g++ make

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY .env .env

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run app.py when the container launches
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "api.app:gunicorn_app"]
