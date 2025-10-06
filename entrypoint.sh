#!/bin/sh
set -e

# Espera o Postgres ficar disponível
/wait-for-it.sh postgres 5432
echo "Postgres está pronto!"

# Cria a tabela se não existir
echo "Criando tabela ativos_b3 se necessário..."
PGPASSWORD=$PG_PASS psql -h $PG_HOST -U $PG_USER -d $PG_DB -f /app/create_table.sql

# Executa o pipeline principal
python main.py
