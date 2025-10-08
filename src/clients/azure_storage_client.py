from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
import os
from dotenv import load_dotenv

load_dotenv()

class StorageService():

    def __init__(self, container_name):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError("Connection String para o Azure Storage não definida.")
        self.storage_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container = self.storage_service_client.get_container_client(container_name)
        self._ensure_container_exists(container_name)

    def _ensure_container_exists(self, container_name: str):
        container_client = self.storage_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            print(f"Container {container_name} criado com sucesso.")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                pass
            else:
                print(f"Erro ao criar container {container_name}: {e}")


    def upload_blob_file(self, container_name: str, file_complete_path: str, file_content: bytes):
        self._ensure_container_exists(container_name)
        blob_client = self.storage_service_client.get_blob_client(container=container_name, blob=file_complete_path)
        try:
            blob_client.upload_blob(data=file_content, overwrite=False)
            print(f"Blob {file_complete_path} carregado com sucesso no container {container_name}")
        except ResourceExistsError:
            print(f"O blob {file_complete_path} já existe e não foi sobrescrito.")

    def download_blob_file(self, container_name: str, file_name: str):
        blob_client = self.storage_service_client.get_blob_client(container=container_name, blob=file_name)

        try:
            download_stream = blob_client.download_blob()
            return download_stream.readall().decode("utf-8")
        except Exception as e:
            print(f"Erro ao baixar o blob {file_name} do container {container_name}: {e}")
            return None
