# app.py

import streamlit as st
import snowflake.connector
import pandas as pd
from mitosheet.streamlit.v1 import spreadsheet

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

# Função para listar os schemas disponíveis no Snowflake em um database específico
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

# Função para adicionar um novo registro na tabela
def adicionar_registro_snowflake(usuario, senha, conta, warehouse, database, schema, tabela, valores):
    conn = snowflake.connector.connect(
        user=usuario,
        password=senha,
        account=conta,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    cursor = conn.cursor()
    
    # Montar a query de inserção
    colunas = ", ".join(valores.keys())
    valores_str = ", ".join(f"'{v}'" for v in valores.values())
    query = f"INSERT INTO {tabela} ({colunas}) VALUES ({valores_str})"

    # Executar a query de inserção
    cursor.execute(query)

    # Commit para efetivar a transação
    conn.commit()

    # Fechar a conexão
    conn.close()
    
# Título do aplicativo
st.title("DSaaS - Central de Dados")

# Adiciona a imagem no canto superior esquerdo
# image_url = "https://raw.githubusercontent.com/seu-usuario/seu-repositorio/master/caminho/para/imagem.png"
# st.sidebar.image(image_url, width=327, height=154)

# Caixas de texto e comboboxes na barra lateral
with st.sidebar:
    st.header("Configurações do Snowflake")
    usuario = st.text_input("User:")
    senha = st.text_input("Password:", type="password")
    conta = st.text_input("Account:")
    warehouse = st.text_input("Warehouse:")

    # Lista de databases disponíveis
    databases_disponiveis = []
    if usuario and senha and conta and warehouse:
        databases_disponiveis = listar_databases(usuario, senha, conta, warehouse)

    # Combobox para selecionar o database
    database = st.selectbox("Database:", databases_disponiveis)

    # Lista de schemas disponíveis
    schemas_disponiveis = []
    if usuario and senha and conta and warehouse and database:
        schemas_disponiveis = listar_schemas(usuario, senha, conta, warehouse, database)

    # Combobox para selecionar o schema
    schema = st.selectbox("Schema:", schemas_disponiveis)

    # Lista de tabelas disponíveis
    tabelas_disponiveis = []
    if usuario and senha and conta and warehouse and database and schema:
        tabelas_disponiveis = listar_tabelas(usuario, senha, conta, warehouse, database, schema)

    # Combobox para selecionar a tabela
    tabela = st.selectbox("Table:", tabelas_disponiveis)

# Botões para obter e inserir dados
botao_inserir_dados, botao_obter_dados = st.columns(2)

# Botão para inserir dados
with botao_inserir_dados:
    if st.button("Inserir linhas"):
        # Caixas de texto vinculadas aos campos da tabela
        if tabela:
            # st.header(f"Adicionar Registro em {tabela}")

            # Obter a lista de colunas da tabela
            colunas_tabela = []
            if usuario and senha and conta and warehouse and database and schema and tabela:
                df = obter_dados_snowflake(usuario, senha, conta, warehouse, database, schema, tabela)
                colunas_tabela = df.columns.tolist()

            # Criar um dicionário para armazenar os valores inseridos pelo usuário
            valores_usuario = {}
            
            # Criar caixas de texto vinculadas aos campos da tabela
            for coluna in colunas_tabela:
                valor = st.text_input(f"{coluna.capitalize()}:")
                valores_usuario[coluna] = valor

            # Botão para adicionar o registro
            if st.button("Salvar"):
                if all(valores_usuario.values()):
                    # Adicionar o registro no Snowflake
                    adicionar_registro_snowflake(usuario, senha, conta, warehouse, database, schema, tabela, valores_usuario)
                    st.success("Registro adicionado com sucesso!")
                else:
                    st.warning("Preencha todos os campos antes de adicionar o registro.")

# Botão para obter dados
with botao_obter_dados:
    if st.button("Visualizar tabela"):
        if usuario and senha and conta and warehouse and database and schema and tabela:
            # Obter dados do Snowflake
            df = obter_dados_snowflake(usuario, senha, conta, warehouse, database, schema, tabela)

            # Exibir DataFrame abaixo do botão
            # st.write("### Dados da Tabela:")
            # st.dataframe(df)
            new_dfs = spreadsheet(df)
            # Apresenta a mitosheet
            st.write(new_dfs)
        else:
            st.warning("Preencha todas as informações antes de obter os dados.")