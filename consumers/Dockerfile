FROM python:3.9-slim

WORKDIR /app

# Instalamos librerías del sistema necesarias para Postgres
RUN apt-get update && apt-get install -y libpq-dev gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-u", "main.py"]