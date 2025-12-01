FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Usamos -u para que los prints salgan en tiempo real en los logs de Docker
CMD ["python", "-u", "main.py"]
