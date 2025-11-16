import logging
import os
import pymssql

def load_ativos_sql(dados: list, blob_name: str = "N/A"):    
    if not dados:
        logging.warning("[LOADER]: Nenhum dado recebido para carga.")
        return

    create_table_sql = """
    IF OBJECT_ID('dbo.ativos_b3', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.ativos_b3 (
            id INT IDENTITY(1,1) PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            data_negociacao DATE NOT NULL,
            preco_abertura NUMERIC(10, 2),
            preco_fechamento NUMERIC(10, 2),
            preco_maximo NUMERIC(10, 2),
            preco_minimo NUMERIC(10, 2),
            volume_financeiro NUMERIC(20, 4),
            CONSTRAINT UQ_ticker_data UNIQUE (ticker, data_negociacao)
        );
    END
    """
    
    merge_sql = """
    MERGE INTO dbo.ativos_b3 AS target
    USING (SELECT ? AS ticker, ? AS data_negociacao, ? AS preco_abertura, ? AS preco_fechamento, ? AS preco_maximo, ? AS preco_minimo, ? AS volume_financeiro) AS source
    ON (target.ticker = source.ticker AND target.data_negociacao = source.data_negociacao)
    WHEN NOT MATCHED THEN
        INSERT (ticker, data_negociacao, preco_abertura, preco_fechamento, preco_maximo, preco_minimo, volume_financeiro)
        VALUES (source.ticker, source.data_negociacao, source.preco_abertura, source.preco_fechamento, source.preco_maximo, source.preco_minimo, source.volume_financeiro);
    """

    logging.info(f"[LOADER]: Iniciando processo de carga para {len(dados)} registros do blob {blob_name}.")
    
    conn = None
    try:
        conn = pymssql.connect(
            server=os.environ["SQL_SERVER"],
            user=os.environ["SQL_USER"],
            password=os.environ["SQL_PASSWORD"],
            database=os.environ["SQL_DATABASE"]
        )

        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            
            dados_em_tuplas = [
                (
                    d['ticker'], d['data_negociacao'], d['preco_abertura'],
                    d['preco_fechamento'], d['preco_maximo'], d['preco_minimo'],
                    d['volume_financeiro']
                )
                for d in dados
            ]
            
            cur.executemany(merge_sql, dados_em_tuplas)
        
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