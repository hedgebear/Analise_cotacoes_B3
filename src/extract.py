from datetime import datetime, timedelta
from pathlib import Path
from src.utils import convert_to_yymmdd
import requests
import os
import zipfile
from src.clients.azure_storage_client import StorageService
import io

def build_url_download(date_to_download:datetime):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{date_to_download}.zip"

def try_http_download(url:str):
    session = requests.session()
    print(f"Tentando baixar: {url}")
    try:
        response = session.get(url, timeout=10)
        print(f"Status code: {response.status_code}")
        if not response.ok:
            print(f"Resposta não OK: {response.status_code} - {response.reason}")
        response.raise_for_status()
        if (response.ok) and response.content and len(response.content) > 200:
            if (response.content[:2] == b'PK'):
                print("Arquivo ZIP válido recebido.")
                return response.content
            else:
                print("Conteúdo recebido não é um arquivo ZIP.")
        else:
            print("Conteúdo recebido é vazio.")
    except requests.RequestException as e:
        print(f"Falha ao acessar a {url}: {e}")
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

PATH_TO_SAVE = "./dados_b3/PREGAO_RAW"

CONTAINER_NAME = "pregao-raw"

def run(qtd_dias_anteriores_a_baixar: int):
    storage_service = StorageService(CONTAINER_NAME)

    dt_inicial = datetime.now().date()
    
    for dias_atras in range(0, qtd_dias_anteriores_a_baixar + 1):
        dt_request = dt_inicial - timedelta(days=dias_atras)
        
        if dt_request.weekday() == 5 or dt_request.weekday() == 6:
            continue 
        
        dt_convertida = convert_to_yymmdd(dt_request)
        
        url_to_download = build_url_download(dt_convertida)
        
        print(f"Tentando baixar arquivo para a data {dt_request} -> {url_to_download}")
        
        zip_bytes = try_http_download(url_to_download)

        if not zip_bytes:
            print(f"Arquivo não encontrado para a data {dt_request}. Tentando o dia anterior.")
            continue

        file_content = extract_file_from_nested_zip(zip_bytes=zip_bytes)

        file_path = f"/{dt_convertida}/"
        file_name = f"SPRE_{dt_convertida}.xml"

        file_complete_path = file_path + file_name

        storage_service.upload_blob_file(
            container_name=CONTAINER_NAME, 
            file_complete_path=file_complete_path,
            file_content=file_content
        )


def salva_arquivo_local(conteudo_arquivo: str, nome_arquivo: str, path: str):
    diretorio = Path(path)
    diretorio.mkdir(parents=True, exist_ok=True)

    complete_file_path = diretorio / Path(nome_arquivo)

    if not complete_file_path.exists():
        with open(complete_file_path, "wb") as f:
            f.write(conteudo_arquivo)
        print(f"Arquivo {nome_arquivo} salvo com sucesso.")

    print(f"Arquivo {nome_arquivo} já existe.")

if __name__ == "__main__":
    run(7)