import os
import pandas as pd
from dbfread import DBF
from dbctodbf import DBCDecompress   # 🔥 nova biblioteca recomendada

# 🗂️ Pasta com os arquivos .DBC baixados do CNES
input_folder = r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/Code-Projeto/dbc-data-cnes"

# 📁 Pasta onde ficarão os arquivos .CSV
output_folder = r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/Code-Projeto/csv-data-cnes"
os.makedirs(output_folder, exist_ok=True)

# 🔍 Localiza todos os .DBC
dbc_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".dbc")]

if not dbc_files:
    print("⚠ Nenhum arquivo .DBC encontrado.")
    exit()

print(f"📦 {len(dbc_files)} arquivo(s) .DBC encontrado(s). Iniciando conversão...\n")

# Criar um único objeto decompresser (mais rápido)
decompresser = DBCDecompress()

for file_name in dbc_files:
    dbc_path = os.path.join(input_folder, file_name)
    base_name = os.path.splitext(file_name)[0]

    print(f"🔸 Convertendo {file_name}...")

    try:
        # 🔽 Converte DBC → DBF
        dbf_path = os.path.join(output_folder, f"{base_name}.dbf")
        decompresser.decompressFile(dbc_path, dbf_path)

        # 📖 Lê o .dbf com dbfread
        table = DBF(dbf_path, encoding="latin1")
        df = pd.DataFrame(iter(table))

        # 💾 Salva CSV
        csv_path = os.path.join(output_folder, f"{base_name}.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        print(f"✅ Sucesso: {file_name} → {csv_path}\n")

    except Exception as e:
        print(f"❌ Erro ao converter {file_name}: {e}\n")

# 🧹 Removendo arquivos .DBF temporários
for f in os.listdir(output_folder):
    if f.lower().endswith(".dbf"):
        try:
            os.remove(os.path.join(output_folder, f))
            print(f"🧹 Arquivo temporário removido: {f}")
        except Exception as e:
            print(f"⚠ Erro ao remover {f}: {e}")

print("🎉 Conversão finalizada com sucesso!")
