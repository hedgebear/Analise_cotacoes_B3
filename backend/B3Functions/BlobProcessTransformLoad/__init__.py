# B3Functions/BlobProcessTransformLoad/__init__.py
import logging
import azure.functions as func
from backend.B3Functions.src import b3_transformer, b3_loader

def main(blob: func.InputStream):
    logging.info(f"Função BlobProcessTransformLoad acionada para o blob: {blob.name}")

    try:
        # 1. Ler o conteúdo do blob
        conteudo_xml_bytes = blob.read()
        logging.info(f"Blob {blob.name} lido com sucesso ({len(conteudo_xml_bytes)} bytes).")

        # 2. Transformar (Parser do XML)
        # A função extrai_dados_xml espera bytes
        dados_transformados = b3_transformer.extrai_dados_xml(conteudo_xml_bytes)

        if not dados_transformados:
            logging.warning(f"Nenhum dado válido extraído do blob {blob.name}.")
            return

        # 3. Carregar (Load no Postgres)
        logging.info(f"Iniciando carga de {len(dados_transformados)} registros no banco de dados.")
        b3_loader.load_ativos(dados_transformados)

        logging.info(f"Processamento do blob {blob.name} concluído com sucesso.")

    except Exception as e:
        logging.error(f"Falha crítica ao processar o blob {blob.name}: {e}")
        # Se falhar, a função irá tentar re-executar (baseado nas políticas de retry do Azure Functions)
        raise