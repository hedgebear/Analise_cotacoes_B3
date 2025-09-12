from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

class StorageService():
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not self.connection_string:
            raise ValueError("Connection String para o Azure Storage não definida.")

        self.storage_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def upload_blob_file(self, container_name: str, file_name: str, file_content: bytes):
        blob_client = self.storage_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob(data=file_content, overwrite=True)

        print(f"Blob {file_name} carregado com sucesso no container {container_name}")