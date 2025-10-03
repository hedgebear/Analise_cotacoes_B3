# Dockerfile para pipeline B3
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Espera o Postgres e Azurite estarem prontos antes de rodar o script principal
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

CMD ["/wait-for-it.sh", "postgres:5432", "azurite:10000", "--", "python", "main.py"]
