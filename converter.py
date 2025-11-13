import os
import pandas as pd
import pyodbc

# 🗂️ Caminho da pasta com os arquivos .DBC
input_folder = r"C:/Users/Manuel Sereno/Documents/teste Python/Projeto-Analise-Dados/dbc-data-aih"
# 📁 Caminho da pasta onde os .CSV serão salvos
output_folder = r"C:/Users/Manuel Sereno/Documents/teste Python/Projeto-Analise-Dados/csv-data-aih"

# Cria a pasta de saída se não existir
# os.makedirs(output_folder, exist_ok=True)

# 🔍 Lista todos os arquivos .dbc da pasta
dbc_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".dbc")]

if not dbc_files:
    print("⚠ Nenhum arquivo .DBC encontrado na pasta especificada.")
else:
    print(f"📦 {len(dbc_files)} arquivo(s) .DBC encontrado(s). Iniciando conversão...\n")

# Loop pelos arquivos .DBC
for file_name in dbc_files:
    dbc_path = os.path.join(input_folder, file_name)
    base_name = os.path.splitext(file_name)[0]  # nome sem extensão

    print(f"🔸 Processando banco: {file_name}")

    try:
        # Conexão com o driver Visual FoxPro
        conn_str = (
            r"Driver={Microsoft Visual FoxPro Driver};"
            f"SourceType=DBC;"
            f"SourceDB={dbc_path};"
            "Exclusive=No;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Lista todas as tabelas no banco DBC
        tables = [t.table_name for t in cursor.tables() if t.table_type == "TABLE"]

        if not tables:
            print(f"⚠ Nenhuma tabela encontrada em {file_name}.")
            conn.close()
            continue

        print(f"📋 {len(tables)} tabela(s) encontrada(s): {', '.join(tables)}")

        # Cria subpasta para cada DBC
        subfolder = os.path.join(output_folder, base_name)
        os.makedirs(subfolder, exist_ok=True)

        # Exporta cada tabela
        for table in tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                csv_table_path = os.path.join(subfolder, f"{base_name}_{table}.csv")
                df.to_csv(csv_table_path, index=False, encoding="utf-8-sig")
                print(f"✅ {table} → {csv_table_path}")
            except Exception as e:
                print(f"⚠ Erro ao exportar tabela '{table}' do banco '{file_name}': {e}")

        conn.close()
        print(f"✅ Conversão concluída para {file_name}\n")

    except Exception as e:
        print(f"❌ Erro ao processar {file_name}: {e}\n")

print("\n🎉 Todas as conversões foram concluídas!")
