# app.py

import streamlit as st
import snowflake.connector
import pandas as pd

# Função para listar os databases disponíveis no Snowflake
def listar_databases(usuario, senha, conta, warehouse):
    conn = snowflake.connector.connect(
        user=usuario,
        password=senha,
        account=conta,
        warehouse=warehouse
    )

    cursor = conn.cursor()
    query = "SHOW DATABASES"
    cursor.execute(query)

    # Obter os databases e fechar a conexão
    databases = [row[1] for row in cursor]
    conn.close()

    return databases

# Função para listar os schemas disponíveis no Snowflake
def listar_schemas(usuario, senha, conta, warehouse, database):
    conn = snowflake.connector.connect(
        user=usuario,
        password=senha,
        account=conta,
        warehouse=warehouse,
        database=database
    )

    cursor = conn.cursor()
    query = "SHOW SCHEMAS"
    cursor.execute(query)

    # Obter os schemas e fechar a conexão
    schemas = [row[1] for row in cursor]
    conn.close()

    return schemas

# Função para listar as tabelas disponíveis no Snowflake em um schema específico
def listar_tabelas(usuario, senha, conta, warehouse, database, schema):
    conn = snowflake.connector.connect(
        user=usuario,
        password=senha,
        account=conta,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    cursor = conn.cursor()
    query = "SHOW TABLES"
    cursor.execute(query)

    # Obter as tabelas e fechar a conexão
    tabelas = [row[1] for row in cursor]
    conn.close()

    return tabelas

# Função para conectar ao Snowflake e obter os dados da tabela
def obter_dados_snowflake(usuario, senha, conta, warehouse, database, schema, tabela):
    conn = snowflake.connector.connect(
        user=usuario,
        password=senha,
        account=conta,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    cursor = conn.cursor()
    query = f"SELECT * FROM {tabela}"
    cursor.execute(query)

    # Obter os dados e fechar a conexão
    dados = cursor.fetchall()
    conn.close()

    # Criar DataFrame
    colunas = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(dados, columns=colunas)

    return df

# Título do aplicativo
st.title("App Streamlit com Snowflake")

# Caixas de texto para entrada de dados
usuario = st.text_input("Digite o usuário do Snowflake:")
senha = st.text_input("Digite a senha do Snowflake:", type="password")
conta = st.text_input("Digite o endereço da conta do Snowflake:")
warehouse = st.text_input("Digite o nome do warehouse do Snowflake:")

# Lista de databases disponíveis
databases_disponiveis = []
if usuario and senha and conta and warehouse:
    databases_disponiveis = listar_databases(usuario, senha, conta, warehouse)

# Combobox para selecionar o database
database = st.selectbox("Selecione o database do Snowflake:", databases_disponiveis)

# Lista de schemas disponíveis
schemas_disponiveis = []
if usuario and senha and conta and warehouse and database:
    schemas_disponiveis = listar_schemas(usuario, senha, conta, warehouse, database)

# Combobox para selecionar o schema
schema = st.selectbox("Selecione o schema do Snowflake:", schemas_disponiveis)

# Lista de tabelas disponíveis
tabelas_disponiveis = []
if usuario and senha and conta and warehouse and database and schema:
    tabelas_disponiveis = listar_tabelas(usuario, senha, conta, warehouse, database, schema)

# Combobox para selecionar a tabela
tabela = st.selectbox("Selecione a tabela do Snowflake:", tabelas_disponiveis)

# Verificar se todas as informações foram fornecidas
if st.button("Obter Dados"):
    if usuario and senha and conta and warehouse and database and schema and tabela:
        # Obter dados do Snowflake
        df = obter_dados_snowflake(usuario, senha, conta, warehouse, database, schema, tabela)

        # Exibir DataFrame
        st.write("Dados da Tabela:")
        st.write(df)
    else:
        st.warning("Por favor, forneça todas as informações necessárias.")
