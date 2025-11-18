import logging
import os
import pyodbc

def load_ativos(dados: list, blob_name: str = "N/A"):    
    if not dados:
        logging.warning("[LOADER]: Nenhum dado recebido para carga.")
        return

    create_table_sql = """
    IF OBJECT_ID('ativos_b3', 'U') IS NULL
    BEGIN
        CREATE TABLE ativos_b3 (
            id INT IDENTITY(1,1) PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            data_negociacao DATE NOT NULL,
            preco_abertura NUMERIC(10, 2),
            preco_fechamento NUMERIC(10, 2),
            preco_maximo NUMERIC(10, 2),
            preco_minimo NUMERIC(10, 2),
            preco_medio NUMERIC(10,2),
            quantidade_movimentada BIGINT,
            volume_financeiro NUMERIC(20, 4),
            CONSTRAINT UQ_ticker_data UNIQUE (ticker, data_negociacao)
        );
    END
    """

    create_temp_table_sql = """
    CREATE TABLE #ativos_temp (
        ticker VARCHAR(10) NOT NULL,
        data_negociacao DATE NOT NULL,
        preco_abertura NUMERIC(10, 2),
        preco_fechamento NUMERIC(10, 2),
        preco_maximo NUMERIC(10, 2),
        preco_minimo NUMERIC(10, 2),
        preco_medio NUMERIC(10,2),
        quantidade_movimentada BIGINT,
        volume_financeiro NUMERIC(20, 4)
    );
    """

    insert_temp_sql = """
    INSERT INTO #ativos_temp 
        (ticker, data_negociacao, preco_abertura, preco_fechamento, preco_maximo, preco_minimo, preco_medio, quantidade_movimentada, volume_financeiro)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    merge_sql = """
    MERGE INTO ativos_b3 AS target
    USING #ativos_temp AS source
        ON (target.ticker = source.ticker AND target.data_negociacao = source.data_negociacao)
    WHEN NOT MATCHED THEN
        INSERT (ticker, data_negociacao, preco_abertura, preco_fechamento, preco_maximo, preco_minimo, preco_medio, quantidade_movimentada, volume_financeiro)
        VALUES (source.ticker, source.data_negociacao, source.preco_abertura, source.preco_fechamento, source.preco_maximo, source.preco_minimo, source.preco_medio, source.quantidade_movimentada, source.volume_financeiro);
    """

    logging.info(f"[LOADER]: Iniciando processo de carga para {len(dados)} registros do blob {blob_name}.")
    
    conn = None
    try:
        driver = "{ODBC Driver 18 for SQL Server}" 
        server = os.environ["SQL_SERVER"]
        db = os.environ["SQL_DATABASE"]
        user = os.environ["SQL_USER"]
        pwd = os.environ["SQL_PASSWORD"]

        conn_string = f"DRIVER={driver};SERVER=tcp:{server},1433;DATABASE={db};UID={user};PWD={pwd}"

        conn = pyodbc.connect(conn_string)

        with conn.cursor() as cur:
            cur.execute(create_table_sql)

            cur.execute(create_temp_table_sql)
            
            dados_em_tuplas = [
                (
                    d['ticker'], d['data_negociacao'], d['preco_abertura'],
                    d['preco_fechamento'], d['preco_maximo'], d['preco_minimo'],
                    d['preco_medio'], d['quantidade_movimentada'], d['volume_financeiro']
                )
                for d in dados
            ]
            
            cur.executemany(insert_temp_sql, dados_em_tuplas)
            logging.info(f"[LOADER]: {len(dados_em_tuplas)} registros inseridos na tabela temporária.")

            cur.execute(merge_sql)
            logging.info(f"[LOADER]: MERGE concluído.")
        
        conn.commit()
        logging.info(f"[LOADER]: Carga de {len(dados)} registros (do {blob_name}) concluída com sucesso!")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"[LOADER]: Falha na carga do blob {blob_name}: {e}")
        raise e
    
    finally:
        if conn:
            conn.close()