Projeto de ETL, para a disciplina de Projeto Cloud do IBMEC-RJ em 2025.2, desenvolvido em Python utilizando a infraestrutura da cloud da Azure, e que consiste no desafio de construção de uma solução que faça a extração, transformação, carregamento e visualização dos dados da B3 de cotações de pregão.

Links utilizados:
1. Cotações históricas – Mercado à vista: https://www.b3.com.br/pt_br/market-data-eindices/servicos-de-dados/market-data/historico/mercado-a-vista/cotacoes-historicas/
2. Pesquisa por pregão – Boletim Diário do Mercado: 
https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/marketdata/historico/boletins-diarios/pesquisa-por-pregao/pesquisa-por-pregao/
3. Layout dos arquivos – Pesquisa por pregão: https://www.b3.com.br/pt_br/market-datae-indices/servicos-de-dados/market-data/historico/boletins-diarios/pesquisa-porpregao/layout-dos-arquivos

Funcionalidades atuais:
- Extração por dia dos dados do pregão
- Armazenamento dos dados raw (em .zip) no Azure Blob Storage

Como rodar localmente
---------------------

1) Pré-requisitos
	- Python 3.9+ instalado
	- Azure Functions Core Tools (para executar `func host start` localmente)
	- Azurite (ou Azure Storage Emulator) se quiser emular o Blob Storage localmente
	- (Opcional) SQL Server local ou uma instância acessível para o banco de dados

2) Criar e ativar um ambiente virtual (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -r backend\B3Functions\requirements.txt
```

3) Configurar variáveis de ambiente / `local.settings.json`
	- Copie ou edite `backend\B3Functions\local.settings.json` e defina as suas strings de conexão:
	  - `AzureWebJobsStorage` (p.ex. `UseDevelopmentStorage=true` para Azurite)
	  - `AZURE_SQL_CONNECTION_STRING` (string de conexão ODBC/pyodbc para o SQL Server)

4) Iniciar Azurite (se estiver usando emulação do Storage)

```powershell
# instalar azurite se necessário
npm i -g azurite
# iniciar azurite (abre um endpoint de blob em http://127.0.0.1:10000 por padrão)
azurite --silent --location .azurite
```

5) Executar o Functions host (dentro do diretório do projeto de functions)

```powershell
cd backend\B3Functions
func host start
```

6) Observações rápidas
	- A função timer (`B3TimerExtract`) busca o pregão do dia anterior e salva raw no Blob.
	- A função de Blob Trigger (`BlobProcessTransformLoad`) reage a blobs no container `pregao-raw` e processa o XML para inserir no banco.
	- Garanta que as variáveis de conexão estejam corretas e que o container exista ou que o serviço possa criá-lo.