from src.extract import run as extract_run
from src.transform import transform
from src.load import load_ativos
import time

def main():
    extract_run(qtd_dias_anteriores_a_baixar=7)
    time.sleep(5)
    dados_pregao = transform(qtd_dias_anteriores_a_baixar=7)
    load_ativos(dados_pregao)


if __name__ == "__main__":
    main()
