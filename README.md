Projeto de ETL, para a disciplina de Projeto Cloud do IBMEC-RJ em 2025.2, desenvolvido em Python utilizando a infraestrutura da cloud da Azure, e que consiste no desafio de construção de uma solução que faça a extração, transformação, carregamento e visualização dos dados da B3 de cotações de pregão.

Links utilizados:
1. Cotações históricas – Mercado à vista: https://www.b3.com.br/pt_br/market-data-eindices/servicos-de-dados/market-data/historico/mercado-a-vista/cotacoes-historicas/
2. Pesquisa por pregão – Boletim Diário do Mercado: 
https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/marketdata/historico/boletins-diarios/pesquisa-por-pregao/pesquisa-por-pregao/
3. Layout dos arquivos – Pesquisa por pregão: https://www.b3.com.br/pt_br/market-datae-indices/servicos-de-dados/market-data/historico/boletins-diarios/pesquisa-porpregao/layout-dos-arquivos

Funcionalidades atuais:
- Extração por dia dos dados do pregão
- Armazenamento dos dados raw (em .zip) no Azure Blob Storage
