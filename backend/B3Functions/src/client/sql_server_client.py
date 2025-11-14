import pyodbc
import os
import logging
from dotenv import load_dotenv

load_dotenv()

class SqlServerClient:
    def __init__(self):
        logging.info("[INFO]: Inicializando cliente SQL Server.")
        self.connection_string = os.getenv("AZURE_SQL_CONNECTION_STRING")
        
        if not self.connection_string:
            raise ValueError("AZURE_SQL_CONNECTION_STRING não definida.")

        self.conn = None
        self.cur = None

    def __enter__(self):
        try:
            self.conn = pyodbc.connect(self.connection_string)
            self.cur = self.conn.cursor()
            return self.cur
        except pyodbc.Error as e:
            logging.error(f"Erro ao conectar no SQL Server: {e}")
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logging.error(f"Ocorreu um erro. Fazendo rollback. Erro: {exc_val}")
            self.conn.rollback()
        else:
            self.conn.commit()
        
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()