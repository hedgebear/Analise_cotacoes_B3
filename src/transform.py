import re
from src.clients.azure_storage_client import StorageService
from datetime import datetime, timedelta
from pathlib import Path
from lxml import etree
import io
import json
from src.utils import convert_to_yymmdd

CONTAINER_NAME = "pregao-raw"

def get_azurite_file(dt: str):
    storage_service = StorageService()
    blob_path = f"/{dt}/SPRE_{dt}.xml"
    xml_content = storage_service.download_blob_file(container_name=CONTAINER_NAME, file_name=blob_path)
    return xml_content.encode("utf-8") if xml_content else None

def transform(qtd_dias_anteriores_a_baixar: int):    
    print(f"[INFO]: Processo de transformação iniciado para os últimos {qtd_dias_anteriores_a_baixar} dias.")
    lista_ativos = []
    
    dt_inicial = datetime.now().date()

    for dias_atras in range(0, qtd_dias_anteriores_a_baixar + 1):
        dt_request = dt_inicial - timedelta(days=dias_atras)
        
        if dt_request.weekday() >= 5:
            continue

        dt_convertida = convert_to_yymmdd(dt_request)

        conteudo_bytes = get_azurite_file(dt_convertida)
        
        if not conteudo_bytes:
            print(f"[INFO]: Nenhum arquivo encontrado para {dt_request.strftime('%d-%m-%Y')}, pulando para o dia anterior.")
            continue

        try:
            print(f"[INFO]: Iniciando extração de dados do XML para a data {dt_request.strftime('%d-%m-%Y')}.")
            xml_bytes = io.BytesIO(conteudo_bytes)
            attributes_namespace = "{urn:bvmf.217.01.xsd}"
            context = etree.iterparse(xml_bytes, tag=f"{attributes_namespace}PricRpt", huge_tree=True)
        
            lista_ativos_data = extrai_dados_xml(context=context, attributes_namespace=attributes_namespace)
        
            if lista_ativos_data:
                lista_ativos.extend(lista_ativos_data)
                print(f"[OK]: Foram extraídos {len(lista_ativos_data)} registros de ativos da data {dt_request.strftime('%d-%m-%Y')}.")
            else:
                print(f"[ATENCAO]: Nenhum ativo válido foi extraído do arquivo da data {dt_request.strftime('%d-%m-%Y')}.")

        except Exception as e:
            print(f"[ERRO]: Falha crítica ao processar o XML da data {dt_request.strftime('%d-%m-%Y')}: {e}")
            continue

    print(f"[OK]: Processo de transformação finalizado. Total de {len(lista_ativos)} ativos extraídos.")
    return lista_ativos

def eh_mercado_a_vista(ticker: str):
    padrao = re.compile(r'^[A-Z]{4}[0-9]{1,2}F?$')
    return padrao.match(ticker)

def extrai_dados_xml(context, attributes_namespace):
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

        if not eh_mercado_a_vista(ticker=ticker) or not ticker:
            continue

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

    return dados_extraidos

def salva_json_local(dados: list, nome_arquivo: str, path_arquivo: str):
    diretorio = Path(path_arquivo)
    diretorio.mkdir(parents=True, exist_ok=True)

    complete_file_path = diretorio / Path(nome_arquivo)

    if not complete_file_path.exists():
        with open(complete_file_path, "w") as f:
            json.dump(dados, f, indent=4)
        print(f"Arquivo {nome_arquivo} salvo com sucesso.")

if __name__ == "__main__":
    transform(7)