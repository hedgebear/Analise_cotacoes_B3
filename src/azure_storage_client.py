from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()
CONTAINER_NAME = "pregao-raw"

class StorageService():

    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError("Connection String para o Azure Storage não definida.")
        self.storage_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container = self.storage_service_client.get_container_client(CONTAINER_NAME)
        self._ensure_container_exists(CONTAINER_NAME)

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


    def upload_blob_file(self, container_name: str, file_name: str, file_content: bytes):
        self._ensure_container_exists(container_name)
        blob_client = self.storage_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob(data=file_content, overwrite=True)
        print(f"Blob {file_name} carregado com sucesso no container {container_name}")

    def download_blob_file(self, container_name: str, file_name: str):
        blob_client = self.storage_service_client.get_blob_client(container=container_name, blob=file_name)

        try:
            download_stream = blob_client.download_blob()
            return download_stream.readall().decode("utf-8")
        except Exception as e:
            print(f"Erro ao baixar o blob {file_name} do container {container_name}: {e}")
            return None
