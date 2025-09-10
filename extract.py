from datetime import datetime
from helpers import convert_to_yymmdd
import requests
import os
import zipfile

PATH_TO_SAVE = "./dados_b3"

def build_url_download(date_to_download:datetime):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{date_to_download}.zip"

def try_http_download(url:str):
    session = requests.session()
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()  
        if (response.ok) and response.content and len(response.content) > 200:
            if (response.content[:2] == b'PK'):
                return response.content, os.path.basename(url)
    except requests.RequestException as e:
        print(f"Falha ao acessar a {url}: {e}")
        pass

def run():
    dt = convert_to_yymmdd(datetime.now())
    url_to_download = build_url_download(dt)

    zip_bytes, zip_name = try_http_download(url_to_download)
    if not zip_bytes:
        raise RuntimeError("Falha ao baixar o arquivo de cotações")
    
    print(f"Download do arquivo de cotações realizado com sucesso: {zip_name}")

    os.makedirs(PATH_TO_SAVE, exist_ok=True)
    zip_path = f'{PATH_TO_SAVE}/pregao_{dt}.zip'
    with open(zip_path, "wb") as f:
        f.write(zip_bytes)
    print(f"Arquivo salvo em: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(f'pregao_{dt}')

    with zipfile.ZipFile(f'pregao_{dt}/PR{dt}.zip', "r") as zip_ref:
        zip_ref.extractall(f'PR{dt}')
    
    print(f"Arquivos extraidos do zip com sucesso")

if __name__ == "__main__":
    run()