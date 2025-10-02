from datetime import datetime, timedelta
from pathlib import Path
from lxml import etree
import io
from decimal import Decimal

from utils import convert_to_yymmdd

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


def transform():
    dt = convert_to_yymmdd(datetime.now() - timedelta(days=1))

    conteudo_bytes = get_file("250930")

    if conteudo_bytes:
        xml_bytes = io.BytesIO(conteudo_bytes)

        attributes_namespace = "{urn:bvmf.217.01.xsd}"

        context = etree.iterparse(xml_bytes, tag=f"{attributes_namespace}PricRpt", huge_tree=True)

        dados_extraidos = []

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
                dados_ativo = {
                    'ticker': ticker,
                    'data_negociacao': data_negociacao_el.text if data_negociacao_el is not None else None,
                    
                    'preco_abertura': float(preco_abertura_el.text) if preco_abertura_el is not None and preco_abertura_el.text else None,
                    'preco_fechamento': float(preco_fechamento_el.text) if preco_fechamento_el is not None and preco_fechamento_el.text else None,
                    'preco_maximo': float(preco_maximo_el.text) if preco_maximo_el is not None and preco_maximo_el.text else None,
                    'preco_minimo': float(preco_minimo_el.text) if preco_minimo_el is not None and preco_minimo_el.text else None,
                    'preco_medio': float(preco_medio_el.text) if preco_medio_el is not None and preco_medio_el.text else None,
                    
                    'quantidade_negocios': int(quantidade_movimentada_el.text) if quantidade_movimentada_el is not None and quantidade_movimentada_el.text else None
                }

                dados_extraidos.append(dados_ativo)

            element.clear()
            while element.getprevious() is not None:
                del element.getparent()[0]

    print(f"Processamento concluído. {len(dados_extraidos)} ativos extraídos.")
    if dados_extraidos:
        print("Exemplo do primeiro ativo:", dados_extraidos[0])

if __name__ == "__main__":
    transform()