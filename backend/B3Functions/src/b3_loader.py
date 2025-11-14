import pyodbc
from .client.sql_server_client import SqlServerClient
import logging


def load_ativos(dados):
    """Carrega os dados de ativos no SQL Server.

    Estratégia adotada: cria a tabela caso não exista e usa MERGE por registro
    (executed via executemany) para garantir idempotência/sem erro em caso de
    reprocessamento do mesmo dia.
    """

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

    # Usamos MERGE para inserir somente quando não existir registro com (ticker, data_negociacao)
    merge_sql = """
    MERGE INTO dbo.ativos_b3 AS target
    USING (SELECT ? AS ticker, ? AS data_negociacao, ? AS preco_abertura, ? AS preco_fechamento, ? AS preco_maximo, ? AS preco_minimo, ? AS volume_financeiro) AS source
    ON (target.ticker = source.ticker AND target.data_negociacao = source.data_negociacao)
    WHEN NOT MATCHED THEN
        INSERT (ticker, data_negociacao, preco_abertura, preco_fechamento, preco_maximo, preco_minimo, volume_financeiro)
        VALUES (source.ticker, source.data_negociacao, source.preco_abertura, source.preco_fechamento, source.preco_maximo, source.preco_minimo, source.volume_financeiro);
    """

    if not dados:
        logging.info("[LOADER]: Nenhum dado novo para inserir.")
        return

    logging.info(f"[LOADER]: Iniciando processo de carga para {len(dados)} registros.")

    try:
        sql_server_client = SqlServerClient()

        with sql_server_client as cur:
            # garante que a tabela exista
            cur.execute(create_table_sql)

            # prepara tuplas para executemany (7 parâmetros por registro)
            dados_em_tuplas = [
                (
                    d['ticker'],
                    d['data_negociacao'],
                    d['preco_abertura'],
                    d['preco_fechamento'],
                    d['preco_maximo'],
                    d['preco_minimo'],
                    d['volume_financeiro']
                )
                for d in dados
            ]

            logging.info(f"[LOADER]: Inserindo/mesclando {len(dados_em_tuplas)} registros (MERGE).")
            cur.fast_executemany = True
            cur.executemany(merge_sql, dados_em_tuplas)

        logging.info("[OK]: Carga no banco de dados concluída com sucesso!")

    except (pyodbc.Error, ValueError) as e:
        logging.error(f"[LOADER]: Falha na operação de carga no banco de dados: {e}")
        raise e