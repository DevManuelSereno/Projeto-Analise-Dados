import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# ------------------------------
# Conexão com PostgreSQL
# ------------------------------
def get_conn():
    return psycopg2.connect(
        host="localhost",
        dbname="DataSusBaV2",
        user="postgres",
        password="09092008",
        port=5432
    )

# ------------------------------
# Função genérica: carrega 1 CSV
# ------------------------------
def carregar_csv_para_staging(csv_path, tabela_destino):
    print(f"📥 Carregando {csv_path} para {tabela_destino}...")

    try:
    # 1. Tentativa mais provável: Ponto e vírgula e Latin-1
        df = pd.read_csv(csv_path, sep=';', encoding='latin-1')
    except UnicodeDecodeError:
    # 2. Segunda tentativa: Ponto e vírgula e Windows-1252
        df = pd.read_csv(csv_path, sep=';', encoding='windows-1252')

    # 🔥 Normaliza colunas para lowercase
    df.columns = df.columns.str.lower()

    conn = get_conn()
    cursor = conn.cursor()

    colunas = ",".join(df.columns)
    valores = [tuple(x) for x in df.values]

    sql = f"INSERT INTO {tabela_destino} ({colunas}) VALUES %s"

    try:
        execute_values(cursor, sql, valores)
        conn.commit()
        print(f"✔ {len(df)} registros inseridos em {tabela_destino}")
    except Exception as e:
        print("❌ Erro ao inserir no banco:", e)
    finally:
        cursor.close()
        conn.close()

# ------------------------------
# NOVA FUNÇÃO: carrega TODOS os CSVs de uma pasta
# ------------------------------
def carregar_todos_csvs(pasta, tabela_destino):
    print(f"\n📂 Lendo pasta: {pasta}")

    arquivos = os.listdir(pasta)

    for arquivo in arquivos:
        if arquivo.lower().endswith(".csv"):
            caminho = os.path.join(pasta, arquivo)
            print(f"\n➡ Importando arquivo: {caminho}")
            carregar_csv_para_staging(caminho, tabela_destino)

    print("\n🎯 Finalizado: todos os CSVs foram processados.")