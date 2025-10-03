from src.extract import run as extract_run
from src.transform import transform
import time

def main():
    print("Iniciando extração do arquivo da B3...")
    extract_run()
    print("Extração e upload para Azurite concluídos.")
    print("Aguardando Azurite estar pronto...")
    time.sleep(5)  # Pequeno delay para garantir que o Azurite esteja pronto
    print("Iniciando transformação e carga no banco de dados...")
    transform()
    print("Processo completo!")

if __name__ == "__main__":
    main()
