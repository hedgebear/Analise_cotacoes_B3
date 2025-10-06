# Dockerfile para pipeline B3

FROM python:3.11-slim

# Instala netcat-openbsd para wait-for-it.sh funcionar
RUN apt-get update && apt-get install -y netcat-openbsd postgresql-client && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY wait-for-it.sh /wait-for-it.sh
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /wait-for-it.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
