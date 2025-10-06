from datetime import datetime, timedelta
from pathlib import Path
from src.utils import convert_to_yymmdd
import requests
import os
import zipfile
from src.azure_storage_client import StorageService
import io
import re

def build_url_download(date_to_download:datetime):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{date_to_download}.zip"

def try_http_download(url:str):
    session = requests.session()
    print(f"Tentando baixar: {url}")
    try:
        response = session.get(url, timeout=10)
        print(f"Status code: {response.status_code}")
        print(f"Tamanho do conteúdo: {len(response.content)} bytes")
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
            print("Conteúdo recebido é muito pequeno ou vazio.")
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
                            return final_file_name, file_content

    return None, None

def extract_files_from_zip(zip_bytes):
    files = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for file_name in z.namelist():
            if file_name.endswith('/'):
                continue

            with z.open(file_name) as f:
                files.append((file_name, f.read()))

    return files

PATH_TO_SAVE = "./dados_b3/PREGAO_RAW"

def run():
    storage_service = StorageService()

    max_days = 7
    zip_bytes = None
    dt = None
    for days_ago in range(1, max_days + 1):
        dt_try = convert_to_yymmdd(datetime.now() - timedelta(days=days_ago))
        url_to_download = build_url_download(dt_try)
        print(f"Tentando baixar arquivo para a data: {dt_try} -> {url_to_download}")
        zip_bytes = try_http_download(url_to_download)
        if zip_bytes:
            dt = dt_try
            print(f"Download do arquivo de cotações realizado com sucesso para a data {dt}")
            break
        else:
            print(f"Arquivo não encontrado para a data {dt_try}. Tentando o dia anterior...")

    if not zip_bytes:
        print(f"Falha ao baixar o arquivo de cotações dos últimos {max_days} dias. Veja os logs acima para detalhes.")
        raise RuntimeError("Falha ao baixar o arquivo de cotações")

    file_name, file_content = extract_file_from_nested_zip(zip_bytes=zip_bytes)
    # Força o nome do arquivo para o padrão SPREdt.xml
    file_name = f"SPRE{dt}.xml"
    file_path = f"{PATH_TO_SAVE}/{dt}/{file_name}"
    directory = Path(file_path).parent
    directory.mkdir(parents=True, exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(file_content)

    blob_path = f"{dt}/{file_name}"
    print(f"blob_path usado para upload: '{blob_path}'")
    storage_service.upload_blob_file(container_name="pregao-raw", file_name=blob_path, file_content=file_content)
    print(f"Arquivo {file_name} salvo local e enviado para o Blob Storage.")


if __name__ == "__main__":
    run()