#!/bin/sh
set -e

# Espera o Postgres ficar disponível
/wait-for-it.sh postgres 5432
echo "Postgres está pronto!"

# Executa as migrations do Alembic
alembic upgrade head

# Executa o pipeline principal
python main.py
