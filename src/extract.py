from datetime import datetime, timedelta
from utils import convert_to_yymmdd
import requests
import os
import zipfile
from azure_storage_client import StorageService
import io

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

def extract_files_from_nested_zip(zip_bytes):
    files = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as external_z:
        for external_file_name in external_z.namelist():
            if external_file_name.lower().endswith('.zip'):            
                internal_zip_bytes = external_z.read(external_file_name)

                with zipfile.ZipFile(io.BytesIO(internal_zip_bytes)) as internal_z:            
                    for final_file_name in internal_z.namelist():
                        if final_file_name.endswith('/'):
                            continue
                            
                        file_content = internal_z.read(final_file_name)
                        files.append((final_file_name, file_content))
            
            break
                        
    return files

def extract_files_from_zip(zip_bytes):
    files = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for file_name in z.namelist():
            if file_name.endswith('/'):
                continue

            with z.open(file_name) as f:
                files.append((file_name, f.read()))

    return files

def run():
    storage_service = StorageService()

    dt = convert_to_yymmdd(datetime.now() - timedelta(days=1))
    url_to_download = build_url_download(dt)

    zip_bytes = try_http_download(url_to_download)
    if not zip_bytes:
        raise RuntimeError("Falha ao baixar o arquivo de cotações")

    print(f"Download do arquivo de cotações realizado com sucesso")

    extracted_files = extract_files_from_zip(zip_bytes)
    for file_name, file_content in extracted_files:
        blob_path = f"dados_b3/{dt}/{file_name}"
        storage_service.upload_blob_file(container_name="pregao-raw", file_name=blob_path, file_content=file_content)

    print("Arquivos enviados para o Blob Storage com sucesso.")

if __name__ == "__main__":
    run()