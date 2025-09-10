from datetime import datetime
from helpers import convert_to_yymmdd
import requests
import os
import zipfile
from azure.storage.blob import BlobServiceClient
import io

PATH_TO_SAVE = "./dados_b3"
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "raw"
PATH_IN_BLOB = "b3_data"

def build_url_download(date_to_download:datetime):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{date_to_download}.zip"

def try_http_download(url:str):
    session = requests.session()
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()  
        if (response.ok) and response.content and len(response.content) > 200:
            if (response.content[:2] == b'PK'):
                return response.content
    except requests.RequestException as e:
        print(f"Falha ao acessar a {url}: {e}")
        pass

def upload_to_blob(file_content: bytes, blob_name: str):
    if not AZURE_STORAGE_CONNECTION_STRING:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING não definida.")
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    blob_client.upload_blob(file_content, overwrite=True)
    print(f"Upload concluído: {blob_name}")

def extract_files_from_zip(zip_bytes):
    files = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for file_name in z.namelist():
            if file_name.endswith('/'):  # pula diretórios
                continue
            with z.open(file_name) as f:
                files.append((file_name, f.read()))
    return files

def run():
    dt = convert_to_yymmdd(datetime.now())
    url_to_download = build_url_download(dt)

    zip_bytes = try_http_download(url_to_download)
    if not zip_bytes:
        raise RuntimeError("Falha ao baixar o arquivo de cotações")

    print(f"Download do arquivo de cotações realizado com sucesso")

    # Extrai arquivos do ZIP em memória
    extracted_files = extract_files_from_zip(zip_bytes)
    for file_name, file_content in extracted_files:
        blob_path = f"{PATH_IN_BLOB}/{dt}/{file_name}"  # opcional: organiza por data no blob
        upload_to_blob(file_content, blob_path)

    print("Todos os arquivos foram enviados para o Blob Storage com sucesso.")

if __name__ == "__main__":
    run()