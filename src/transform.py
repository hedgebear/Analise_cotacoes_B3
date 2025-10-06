from src.azure_storage_client import StorageService
from datetime import datetime, timedelta
from pathlib import Path
from lxml import etree
import io
import json

from src.db_client import insert_ativos
from src.utils import convert_to_yymmdd

RAW_FILE_PATH = Path("./dados_b3/PREGAO_RAW")
PATH_TO_SAVE = "./dados_b3/PREGAO_TRANSFORMED"

def get_file(dt: str):
    diretorio_data = RAW_FILE_PATH / dt

    if not diretorio_data.is_dir():
        print("Diretório não encontrado")
        return None
    
    arquivo_xml = list(diretorio_data.glob('*.xml'))[0]
    
    if not arquivo_xml:
        print(f"AVISO: Nenhum arquivo .xml encontrado no diretório '{diretorio_data}'")
        return None
        
    return arquivo_xml.read_bytes()

def get_file_from_azurite(dt: str):
    storage_service = StorageService()
    blob_name = f"{dt}/SPRE{dt}.xml"   # ajuste se o nome for diferente
    xml_content = storage_service.download_blob_file(container_name="pregao-raw", file_name=blob_name)
    return xml_content.encode("utf-8") if xml_content else None


def transform():

    max_days = 7
    conteudo_bytes = None
    dt = None
    for days_ago in range(1, max_days + 1):
        dt_try = convert_to_yymmdd(datetime.now() - timedelta(days=days_ago))
        print(f"Tentando buscar XML para a data: {dt_try}")
        conteudo_bytes = get_file_from_azurite(dt_try)
        if conteudo_bytes:
            dt = dt_try
            print(f"Arquivo XML encontrado para a data {dt}")
            break
        else:
            print(f"Arquivo XML não encontrado para a data {dt_try}. Tentando o dia anterior...")

    dados_extraidos = []

    if conteudo_bytes:
        xml_bytes = io.BytesIO(conteudo_bytes)

        attributes_namespace = "{urn:bvmf.217.01.xsd}"

        context = etree.iterparse(xml_bytes, tag=f"{attributes_namespace}PricRpt", huge_tree=True)

        for _, element in context:
            ticker_el = element.find(f"{attributes_namespace}SctyId/{attributes_namespace}TckrSymb")
            data_negociacao_el = element.find(f"{attributes_namespace}TradDt/{attributes_namespace}Dt")
            preco_abertura_el = element.find(f"{attributes_namespace}FinInstrmAttrbts/{attributes_namespace}FrstPric")
            preco_fechamento_el = element.find(f"{attributes_namespace}FinInstrmAttrbts/{attributes_namespace}LastPric")
            preco_maximo_el = element.find(f"{attributes_namespace}FinInstrmAttrbts/{attributes_namespace}MaxPric")
            preco_minimo_el = element.find(f"{attributes_namespace}FinInstrmAttrbts/{attributes_namespace}MinPric")
            preco_medio_el = element.find(f"{attributes_namespace}FinInstrmAttrbts/{attributes_namespace}TradAvrgPric")
            quantidade_movimentada_el = element.find(f"{attributes_namespace}FinInstrmAttrbts/{attributes_namespace}RglrTxsQty")

            ticker = ticker_el.text if ticker_el is not None else None

            if ticker:
                data_negociacao = data_negociacao_el.text if data_negociacao_el is not None and data_negociacao_el.text else None
                preco_abertura = float(preco_abertura_el.text) if preco_abertura_el is not None and preco_abertura_el.text else None
                preco_fechamento = float(preco_fechamento_el.text) if preco_fechamento_el is not None and preco_fechamento_el.text else None
                preco_maximo = float(preco_maximo_el.text) if preco_maximo_el is not None and preco_maximo_el.text else None
                preco_minimo = float(preco_minimo_el.text) if preco_minimo_el is not None and preco_minimo_el.text else None
                preco_medio = float(preco_medio_el.text) if preco_medio_el is not None and preco_medio_el.text else None
                quantidade_negocios = int(quantidade_movimentada_el.text) if quantidade_movimentada_el is not None and quantidade_movimentada_el.text else None
                volume_financeiro = 0
                if preco_medio and quantidade_negocios:
                    volume_financeiro = preco_medio * quantidade_negocios
                dados_ativo = {
                    'ticker': ticker,
                    'data_negociacao': data_negociacao,
                    'preco_abertura': preco_abertura,
                    'preco_fechamento': preco_fechamento,
                    'preco_maximo': preco_maximo,
                    'preco_minimo': preco_minimo,
                    'volume_financeiro': round(volume_financeiro, 4)
                }
                dados_extraidos.append(dados_ativo)
            element.clear()
            while element.getprevious() is not None:
                del element.getparent()[0]

        # Salva como dt/SPREdt.json para padronizar igual ao XML
        file_path = f"{PATH_TO_SAVE}/{dt}/SPRE{dt}.json"
        directory = Path(file_path).parent
        directory.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w') as file:
            json.dump(dados_extraidos, file, indent=4)
    
    if dados_extraidos:
        insert_ativos(dados_extraidos)
        print(f"Inseridos {len(dados_extraidos)} registros no PostgreSQL")

    print(f"Processamento concluído. {len(dados_extraidos)} ativos extraídos.")
    if dados_extraidos:
        print("Exemplo do primeiro ativo:", dados_extraidos[0])
    elif dt is None:
        print(f"Nenhum arquivo XML encontrado nos últimos {max_days} dias.")

if __name__ == "__main__":
    transform()