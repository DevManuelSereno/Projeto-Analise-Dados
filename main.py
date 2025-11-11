import pandas as pd
from dbfread import DBF
import glob
import os

print("Iniciando processo de carga dos arquivos .DBF...")

# --- 1. CONFIGURE O CAMINHO ---
# Coloque todos os seus arquivos .DBF (ex: RDBA201901.dbf, RDBA201902.dbf, etc.)
# nesta pasta.
caminho_dados = "C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto Análise de Dados/dados-CNES"

# --- 2. ENCONTRAR TODOS OS ARQUIVOS .DBF ---
# O glob vai criar uma lista com o nome de todos os arquivos .DBF na pasta
padrao_arquivos = os.path.join(caminho_dados, '*.DBF')
lista_arquivos_dbf = glob.glob(padrao_arquivos)

if not lista_arquivos_dbf:
    print(f"AVISO: Nenhum arquivo .DBF encontrado em {caminho_dados}")
    print("Por favor, verifique o caminho e se os arquivos existem.")
else:
    print(f"Encontrados {len(lista_arquivos_dbf)} arquivos .DBF.")

# --- 3. LER E COMBINAR OS ARQUIVOS ---
lista_dataframes = []

for arquivo in lista_arquivos_dbf:
    try:
        nome_base = os.path.basename(arquivo)
        print(f"Processando: {nome_base}...")
        
        # Carrega o arquivo DBF.
        # A codificação 'latin1' é muito comum para dados do DATASUS.
        # Se der erro, tente 'utf-8' ou 'iso-8859-1'.
        dbf = DBF(arquivo, encoding='latin1')
        
        # Converte para DataFrame do Pandas
        df = pd.DataFrame(iter(dbf))
        
        lista_dataframes.append(df)
        print(f" -> {len(df)} registros carregados de {nome_base}.")
        
    except Exception as e:
        print(f"ERRO ao processar o arquivo {arquivo}: {e}")

# --- 4. CRIAR O DATAFRAME FINAL ---
if lista_dataframes:
    print("\nCombinando todos os DataFrames...")
    # Concatena todos os dataframes da lista em um único
    df_final = pd.concat(lista_dataframes, ignore_index=True)
    
    print("==========================================================")
    print(f"PROCESSAMENTO CONCLUÍDO!")
    print(f"Total de registros combinados: {len(df_final)}")
    print("Amostra dos dados (primeiras 5 linhas):")
    print(df_final.head())
    print("\nColunas carregadas:")
    print(df_final.info())
    print("==========================================================")


    # --- 5. (OPCIONAL) SALVAR O RESULTADO COMBINADO ---
    # Agora que você tem tudo em um DataFrame, pode salvar 
    # em um formato mais moderno para não ter que ler os .DBF de novo.
    
    # Opção A: Salvar como CSV único
    print("\nSalvando em CSV (pode demorar)...")
    df_final.to_csv("cnes_combinado_2019_2024.csv", index=False, encoding='utf-8')
    print("Arquivo 'cnes_combinado_2019_2024.csv' salvo.")

    # Opção B: Salvar como Parquet (RECOMENDADO)
    # É mais rápido e ocupa menos espaço que CSV.
    # Pode precisar de 'pip install pyarrow'
#     try:
#         print("\nSalvando em Parquet (mais rápido)...")
#         df_final.to_parquet("aih_combinado_2019_2024.parquet", index=False)
#         print("Arquivo 'aih_combinado_2019_2024.parquet' salvo.")
#     except ImportError:
#         print("\nPara salvar em Parquet, instale 'pyarrow': pip install pyarrow")

# else:
#     print("Nenhum dado foi carregado. Encerrando o script.")