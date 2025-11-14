import azure.functions as func
import logging
from datetime import datetime, timedelta
from backend.B3Functions.src import b3_extractor

def main(timer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc
    ).isoformat()

    if timer.past_due:
        logging.info('O timer está atrasado!')

    logging.info('Função B3TimerExtract acionada com sucesso.')

    # Pega os dados de T-1 (dia anterior)
    # A própria função b3_extractor já verifica se é fim de semana.
    ontem = datetime.now().date() - timedelta(days=1)
    
    try:
        sucesso = b3_extractor.run_extraction(ontem)
        if sucesso:
            logging.info(f"Extração para {ontem.strftime('%d-%m-%Y')} concluída com sucesso.")
        else:
            logging.warning(f"Extração para {ontem.strftime('%d-%m-%Y')} não foi executada (provavelmente fim de semana ou falha).")
    except Exception as e:
        logging.error(f"Falha crítica ao executar b3_extractor para {ontem.strftime('%d-%m-%Y')}: {e}")

    logging.info('Função B3TimerExtract finalizada.')