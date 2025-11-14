from datetime import datetime, timedelta
from .utils import convert_to_yymmdd
import requests
import zipfile
from .client.azure_storage_client import StorageService
import io
import logging

CONTAINER_NAME = "pregao-raw"

def build_url_download(date_to_download:datetime):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{date_to_download}.zip"

def try_http_download(url:str):
    session = requests.session()
    logging.info(f"[EXTRACTOR]: Iniciando download de '{url}'.")
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()

        if response.content and len(response.content) > 200:
            if response.content.startswith(b'PK'):
                logging.info(f"[EXTRACTOR]: Arquivo ZIP válido recebido de '{url}'.")
                return response.content
            else:
                logging.warning(f"[EXTRACTOR]: Conteúdo de '{url}' não é um arquivo ZIP.")
        else:
            logging.warning(f"[EXTRACTOR]: Conteúdo de '{url}' está vazio ou é muito pequeno.")
    except requests.exceptions.HTTPError as e:
        logging.error(f"[EXTRACTOR]: Falha no download de '{url}' (HTTP Error): {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"[EXTRACTOR]: Falha de conexão/timeout ao acessar '{url}': {e}")
    
    return None

def extract_file_from_nested_zip(zip_bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as external_z:
        for external_file_name in external_z.namelist():
            if external_file_name.lower().endswith('.zip'):
                internal_zip_bytes = external_z.read(external_file_name)

                with zipfile.ZipFile(io.BytesIO(internal_zip_bytes)) as internal_z:
                    for final_file_name in internal_z.namelist():
                        if not final_file_name.endswith('/'):
                            file_content = internal_z.read(final_file_name)
                            return file_content
    return None

def run_extraction(dt_request: datetime.date):
    """
    Executa a extração para uma data específica.
    """
    storage_service = StorageService()
    
    if dt_request.weekday() >= 5:
        logging.info(f"[EXTRACTOR]: Data {dt_request.strftime('%d-%m-%Y')} é um fim de semana. Pulando.")
        return False

    dt_convertida = convert_to_yymmdd(dt_request)
    url_to_download = build_url_download(dt_convertida)
    
    logging.info(f"[EXTRACTOR]: Buscando arquivo para a data {dt_request.strftime('%d-%m-%Y')}.")
    
    zip_bytes = try_http_download(url_to_download)

    if not zip_bytes:
        logging.warning(f"[EXTRACTOR]: Nenhum arquivo válido retornado para {dt_request.strftime('%d-%m-%Y')}.")
        return False

    try:
        file_content = extract_file_from_nested_zip(zip_bytes=zip_bytes)
        logging.info(f"[EXTRACTOR]: Arquivo extraído com sucesso para a data {dt_request.strftime('%d-%m-%Y')}.")
    except Exception as e:
        logging.error(f"[EXTRACTOR]: Falha ao extrair o arquivo ZIP da data {dt_request.strftime('%d-%m-%Y')}: {e}")
        return False

    file_path = f"/{dt_convertida}/"
    file_name = f"SPRE_{dt_convertida}.xml"
    file_complete_path = file_path + file_name

    storage_service.upload_blob_file(
        container_name=CONTAINER_NAME, 
        file_complete_path=file_complete_path,
        file_content=file_content
    )
    return True