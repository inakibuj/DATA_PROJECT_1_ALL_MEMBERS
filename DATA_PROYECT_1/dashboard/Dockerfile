FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema para conectar a Postgres
RUN apt-get update && apt-get install -y libpq-dev gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Exponemos el puerto 8050 (el estándar de Dash)
EXPOSE 8050

CMD ["python", "app.py"]