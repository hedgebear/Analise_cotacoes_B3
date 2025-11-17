import streamlit as st
import pandas as pd
import pymssql
import os
from dotenv import load_dotenv

st.set_page_config(
    page_title="Dashboard Azure SQL",
    layout="wide"
)

load_dotenv()

SQL_SERVER = os.environ.get("SQL_SERVER")
SQL_DATABASE = os.environ.get("SQL_DATABASE")
SQL_USER = os.environ.get("SQL_USER")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")

@st.cache_data
def carregar_dados_do_sql():
    if not all([SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD]):
        st.error("As variáveis de ambiente do banco de dados (DB_SERVER, DB_DATABASE, DB_USER, DB_PASSWORD) não foram configuradas no Azure.")
        return pd.DataFrame()

    try:
        with pymssql.connect(
            server=SQL_SERVER,
            user=SQL_USER,
            password=SQL_PASSWORD,
            database=SQL_DATABASE
        ) as conn:
            
            query = "SELECT * FROM ativos_b3" 
            
            df = pd.read_sql(query, conn)
            
            if 'data_negociacao' not in df.columns:
                st.error("A coluna 'data_negociacao' não foi encontrada no banco de dados.")
                return pd.DataFrame()
            if 'ticker' not in df.columns:
                st.error("A coluna 'ticker' não foi encontrada no banco de dados.")
                return pd.DataFrame()

            df['data_negociacao'] = pd.to_datetime(df['data_negociacao'])
            
            return df

    except Exception as e:
        st.error(f"Erro ao conectar ou buscar dados no Azure SQL: {e}")
        return pd.DataFrame()

st.title("Dashboard de Ativos da B3")

df = carregar_dados_do_sql()

if df.empty:
    st.warning("Não foi possível carregar os dados. Verifique a query ou a conexão com o banco.")
    st.stop()

st.sidebar.header("Filtros")

lista_ativos = df['ticker'].unique()
ativo_selecionado = st.sidebar.selectbox(
    "Selecione o Ativo:",
    lista_ativos
)

data_min = df['data_negociacao'].min().date()
data_max = df['data_negociacao'].max().date()

data_inicio = st.sidebar.date_input(
    "Data Inicial:",
    value=data_min,
    min_value=data_min,
    max_value=data_max,
    format="DD/MM/YYYY"
)

data_fim = st.sidebar.date_input(
    "Data Final:",
    value=data_max,
    min_value=data_min,
    max_value=data_max,
    format="DD/MM/YYYY"
)

if data_fim < data_inicio:
    st.sidebar.error("A Data Final não pode ser anterior à Data Inicial.")
    st.stop()


df_filtrado = df[
    (df['ticker'] == ativo_selecionado) &
    (df['data_negociacao'].dt.date >= data_inicio) &
    (df['data_negociacao'].dt.date <= data_fim)
].copy()

df_filtrado = df_filtrado.sort_values(by='data_negociacao')

st.header(f"Análise do Ativo: {ativo_selecionado}")
st.write(f"Mostrando dados de {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")

if df_filtrado.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
else:
    st.subheader("Tabela de Variações Diárias")
    st.dataframe(
        df_filtrado,
        hide_index=True,
        column_config={
            "id": None,
            "data_negociacao": st.column_config.DatetimeColumn(
                format="DD/MM/YYYY"
            )
        }
    )
    
    st.subheader("Variação diária dos preços de fechamento")
    chart_data = df_filtrado.set_index('data_negociacao')
    
    colunas_preco = [col for col in ['preco_fechamento'] if col in chart_data.columns]
    
    if colunas_preco:
        st.line_chart(chart_data[colunas_preco])
    else:
        st.warning("Nenhuma coluna de preço (ex: 'preco_fechamento') encontrada para plotar o gráfico.")