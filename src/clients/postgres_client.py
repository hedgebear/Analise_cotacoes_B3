import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv

load_dotenv()

class PostgresClient():

    def __init__(self):
        print("INFO: Inicializando cliente PostgreSQL.")
        self.postgres_config = {
            "host": os.getenv("PG_HOST", "localhost"),
            "port": os.getenv("PG_PORT", "5432"),
            "dbname": os.getenv("PG_DB"),
            "user": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASS")
        }

        if not self.postgres_config["dbname"] or not self.postgres_config["password"]:
            raise ValueError("As variáveis PG_DB e PG_PASS devem ser definidas.")

        self.conn = None
        self.cur = None

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(**self.postgres_config)
            self.cur = self.conn.cursor()
            return self.cur
        except psycopg2.OperationalError as e:
            print(f"Erro ao conectar no banco de dados: {e}")
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print(f"Ocorreu um erro. Fazendo rollback. Erro: {exc_val}")
            self.conn.rollback()
        else:
            self.conn.commit()
        
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
