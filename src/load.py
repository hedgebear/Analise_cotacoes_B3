import psycopg2
from src.clients.postgres_client import PostgresClient
from psycopg2.extras import execute_batch

def load_ativos(dados):
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS ativos_b3 (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            data_negociacao DATE NOT NULL,
            preco_abertura NUMERIC(10, 2),
            preco_fechamento NUMERIC(10, 2),
            preco_maximo NUMERIC(10, 2),
            preco_minimo NUMERIC(10, 2),
            volume_financeiro NUMERIC(10, 2),
            UNIQUE (ticker, data_negociacao)
        );
    """

    insert_sql = """
        INSERT INTO ativos_b3 
        (ticker, data_negociacao, preco_abertura, preco_fechamento, preco_maximo, preco_minimo, volume_financeiro)
        VALUES (%(ticker)s, %(data_negociacao)s, %(preco_abertura)s, %(preco_fechamento)s, %(preco_maximo)s, %(preco_minimo)s, %(volume_financeiro)s)
        ON CONFLICT (ticker, data_negociacao) DO NOTHING
    """

    print(f"[INFO]: Iniciando processo de carga para {len(dados)} registros de ativos.")

    try:
        postgres_client = PostgresClient()

        with postgres_client as cur:
            cur.execute(create_table_sql)

            if dados:
                print(f"[INFO]: Iniciando inserção de {len(dados)} registros em lote.")
                execute_batch(cur, insert_sql, dados, page_size=200)
                print(f"[OK]: Inserção em lote finalizada.")
            else:
                print("[INFO]: Nenhum dado novo para inserir. Etapa de inserção pulada.")

        print(f"[OK]: Carga no banco de dados concluída com sucesso!")

    except (ValueError, psycopg2.Error) as e:
        print(f"[ERRO]: Falha na operação de carga no banco de dados: {e}")
        raise