import re
from lxml import etree
import io
import logging

def eh_mercado_a_vista(ticker: str):
    padrao = re.compile(r'^[A-Z]{4}[0-9]{1,2}F?$')
    return padrao.match(ticker)

def extrai_dados_xml(conteudo_bytes: bytes):
    """
    Extrai dados de um único arquivo XML (bytes).
    """
    dados_extraidos = []
    
    try:
        logging.info("[TRANSFORMER]: Iniciando extração de dados do XML.")
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

        logging.info(f"[TRANSFORMER]: Foram extraídos {len(dados_extraidos)} registros de ativos do XML.")

    except Exception as e:
        logging.error(f"[TRANSFORMER]: Falha crítica ao processar o XML: {e}")
        return []

    return dados_extraidos