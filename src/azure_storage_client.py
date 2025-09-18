from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()
CONTAINER_NAME = "pregao_raw"

class StorageService():
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not self.connection_string:
            raise ValueError("Connection String para o Azure Storage não definida.")

        self.storage_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container = self.storage_service_client.get_container_client(CONTAINER_NAME)

    def upload_blob_file(self, container_name: str, file_name: str, file_content: bytes):

        try:
            self.storage_service_client.create_container(container_name)
            print(f"Container {container_name} criado com sucesso.")
        except Exception as e:
            print(f"Container {container_name} já existe ou erro ao criar: {e}")
            pass

        blob_client = self.storage_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob(data=file_content, overwrite=True)

        print(f"Blob {file_name} carregado com sucesso no container {container_name}")