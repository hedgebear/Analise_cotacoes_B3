from datetime import datetime, timedelta
from backend.B3Functions.src.client.azure_storage_client import StorageService
from backend.B3Functions.src.b3_transformer import extrai_dados_xml
from backend.B3Functions.src.b3_loader import load_ativos as load_ativos_sql
from backend.B3Functions.src.utils import convert_to_yymmdd

dias_pra_buscar = 60
data_inicial = datetime.now().date() - timedelta(1)
jsons_lista = []

storage_client = StorageService()
CONTAINER_NAME = 'pregao-raw'

for i in range(dias_pra_buscar):
    data_alvo = data_inicial - timedelta(i)

    dt_convertida = convert_to_yymmdd(data_alvo)
    path_blob = f"{dt_convertida}/SPRE_{dt_convertida}.xml"

    conteudo = storage_client.download_blob_file(container_name=CONTAINER_NAME, file_name=path_blob)

    try:
        jsons_dia = extrai_dados_xml(conteudo_bytes=conteudo)

        if jsons_dia:
            jsons_lista.extend(jsons_dia)

    except Exception as e:
        print(f"Erro ao processar o dia {data_alvo}")

if jsons_lista:
    load_ativos_sql(jsons_lista)
else:
    print("Nenhum dado encontrado no período")

