import logging
import os
import pymssql

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
    
    merge_sql = """
    MERGE INTO ativos_b3 AS target
    USING (SELECT %s AS ticker, %s AS data_negociacao, %s AS preco_abertura, %s AS preco_fechamento, %s AS preco_maximo, %s AS preco_minimo, %s AS preco_medio, %s AS quantidade_movimentada, %s AS volume_financeiro) AS source
    ON (target.ticker = source.ticker AND target.data_negociacao = source.data_negociacao)
    WHEN NOT MATCHED THEN
        INSERT (ticker, data_negociacao, preco_abertura, preco_fechamento, preco_maximo, preco_minimo, preco_medio, quantidade_movimentada, volume_financeiro)
        VALUES (source.ticker, source.data_negociacao, source.preco_abertura, source.preco_fechamento, source.preco_maximo, source.preco_minimo, source.preco_medio, source.quantidade_movimentada, source.volume_financeiro);
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
                    d['preco_medio'], d['quantidade_movimentada'], d['volume_financeiro']
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