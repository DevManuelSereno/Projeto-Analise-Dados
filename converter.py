import os
import pandas as pd
from dbfread import DBF
from datasus_dbc import decompress  # 👈 importa o método para descompactar

# 🗂️ Caminho da pasta com os arquivos .DBC
input_folder = r"C:\Users\User\Desktop\Projeto-Analise-Dados\dados"
# 📁 Caminho da pasta onde os .CSV serão salvos
output_folder = r"C:\Users\User\Desktop\Projeto-Analise-Dados\dadoscsv"

# Cria a pasta de saída se não existir
os.makedirs(output_folder, exist_ok=True)

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

    print(f"🔸 Convertendo {file_name}...")

    try:
        # 🔽 Descompacta o .dbc para .dbf temporário
        dbf_path = os.path.join(output_folder, f"{base_name}.dbf")
        decompress(dbc_path, dbf_path)

        # 📖 Lê o arquivo .dbf com dbfread
        table = DBF(dbf_path, encoding="latin1")
        df = pd.DataFrame(iter(table))

        # 💾 Salva como CSV
        csv_path = os.path.join(output_folder, f"{base_name}.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        print(f"✅ {file_name} → {csv_path}")

    except Exception as e:
        print(f"❌ Erro ao converter {file_name}: {e}")

print("\n🎉 Todas as conversões foram concluídas!")
