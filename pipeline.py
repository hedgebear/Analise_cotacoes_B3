from src.extract import run as extract_run
from src.transform import transform
from src.load import load_ativos
import time

def main():
    print("Iniciando extração dos arquivos da B3.")
    extract_run(qtd_dias_anteriores_a_baixar=7)
    print("Extração e upload para Azurite concluídos.")
    print("Aguardando Azurite estar pronto.")
    time.sleep(5)
    print("Iniciando transformação e carga no banco de dados.")
    dados_pregao = transform(qtd_dias_anteriores_a_baixar=7)
    load_ativos(dados_pregao)


if __name__ == "__main__":
    main()
