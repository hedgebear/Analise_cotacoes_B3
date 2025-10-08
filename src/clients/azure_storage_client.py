from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import os
from dotenv import load_dotenv

load_dotenv()

class StorageService():
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError("[ERRO]: Connection string para o Azure Storage não definida.")
        
        self.storage_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def upload_blob_file(self, container_name: str, file_complete_path: str, file_content: bytes):
        self._ensure_container_exists(container_name=container_name)

        blob_client = self.storage_service_client.get_blob_client(container=container_name, blob=file_complete_path)
        try:
            blob_client.upload_blob(data=file_content, overwrite=False)
            print(f"[OK]: Iniciando upload do blob '{file_complete_path}' para o container '{container_name}'.")
        except ResourceExistsError:
            print(f"[INFO]: O blob '{file_complete_path}' já existe e não foi sobrescrito.")
        except Exception as e:
            print(f"[ERRO]: Falha inesperada no upload do blob '{file_complete_path}': {e}")

    def download_blob_file(self, container_name: str, file_name: str):
        print(f"[INFO]: Iniciando download do blob '{file_name}' do container '{container_name}'.")
        blob_client = self.storage_service_client.get_blob_client(container=container_name, blob=file_name)

        try:
            download_stream = blob_client.download_blob()
            content = download_stream.readall().decode("utf-8")
            print(f"SUCCESS: Blob '{file_name}' baixado com sucesso do container '{container_name}'.")
            return content
        except ResourceNotFoundError:
            print(f"[ATENCAO]: O blob '{file_name}' não foi encontrado no container '{container_name}'.")
            return None
        except Exception as e:
            print(f"[ERRO]: Falha inesperada no download do blob '{file_name}': {e}")
            return None

    def _ensure_container_exists(self, container_name: str):
        container_client = self.storage_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            print(f"[OK]: Container '{container_name}' criado com sucesso.")
        except ResourceExistsError:
            print(f"[INFO]: Container '{container_name}' já existe.")
        except Exception as e:
                print(f"[ERRO]: Erro ao criar container '{container_name}': {e}")