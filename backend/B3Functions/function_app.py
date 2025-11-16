import azure.function as func
import logging
from datetime import datetime, timedelta

from src.b3_extractor import run_extraction
from src.b3_transformer import extrai_dados_xml
from src.b3_loader import load_ativos

app = func.FunctionApp()

@app.schedule(schedule="0 0 22 * * 1-5",
              arg_name="timer",
              run_on_startup=False)

def extracao_diaria(timer: func.TimerRequest):
    dt_request = datetime.now().date() 
    
    logging.info(f"TIMER TRIGGER: Iniciando extração para a data: {dt_request.strftime('%Y-%m-%d')}")
    
    try:
        success = run_extraction(dt_request)
        if success:
            logging.info(f"TIMER TRIGGER: Extração para {dt_request} concluída e salva no blob.")
        else:
            logging.info(f"TIMER TRIGGER: Extração para {dt_request} pulada ou falhou (ver logs).")
    except Exception as e:
        logging.error(f"TIMER TRIGGER: Falha crítica no 'run_extraction': {e}")


@app.blob_trigger(
    arg_name="blob",
    path="pregao-raw",
    connection_string_setting="AzureWebJobsStorage"
)
def transformacao_carregamento_sql_server(blob: func.InputStream):
    logging.info(f"PIPELINE (BLOB TRIGGER): Iniciando processo para o blob: {blob.blob_name}")

    try:
        logging.info(f"[EXTRACT]: Lendo bytes do blob {blob.blob_name}...")
        conteudo_bytes = blob.read()
        logging.info(f"[EXTRACT]: Concluído. {len(conteudo_bytes)} bytes lidos.")
        
    except Exception as e:
        logging.error(f"[EXTRACT]: Falha ao ler o blob: {e}")
        return

    try:
        logging.info(f"[TRANSFORM]: Iniciando transformação do XML...")
        dados = extrai_dados_xml(conteudo_bytes)
        
        if not dados:
            logging.warning("[TRANSFORM]: Transformação não retornou dados. Encerrando.")
            return
            
        logging.info(f"[TRANSFORM]: Transformação concluída. {len(dados)} registros gerados.")
        
    except Exception as e:
        logging.error(f"[TRANSFORM]: Falha crítica na transformação: {e}")
        return

    try:
        logging.info(f"[LOAD]: Iniciando carga no SQL Server...")
        load_ativos(dados, blob_name=blob.blob_name)
        logging.info(f"[LOAD]: Carga concluída com sucesso.")
        
    except Exception as e:
        logging.error(f"[LOAD]: Falha na etapa de carga: {e}")

    logging.info(f"PIPELINE (BLOB TRIGGER): Processo concluído para o blob: {blob.blob_name}")