import psycopg2
from psycopg2.extras import execute_batch
import os

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "b3data"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASS", "lucasfm2002"),
}

def insert_ativos(rows):
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    sql = """
        INSERT INTO ativos_b3 
        (ticker, data_negociacao, preco_abertura, preco_fechamento, preco_maximo, preco_minimo, volume_financeiro)
        VALUES (%(ticker)s, %(data_negociacao)s, %(preco_abertura)s, %(preco_fechamento)s, %(preco_maximo)s, %(preco_minimo)s, %(volume_financeiro)s)
    """
    execute_batch(cur, sql, rows, page_size=200)
    conn.commit()
    cur.close()
    conn.close()
