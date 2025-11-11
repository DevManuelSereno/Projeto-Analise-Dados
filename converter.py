import pandas as pd
import os
from dbfread import DBF

# Caminho da pasta com os arquivos .DBF
input_folder = r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/dados-CNES"   # <-- coloque o seu caminho real aqui
output_folder = r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS"  # <-- onde salvar os .CSV

# Cria a pasta de saída se não existir
os.makedirs(output_folder, exist_ok=True)

# Loop pelos arquivos .DBF
for file_name in os.listdir(input_folder):
    if file_name.lower().endswith(".dbf"):
        dbf_path = os.path.join(input_folder, file_name)
        csv_name = file_name.replace(".dbf", ".csv")
        csv_path = os.path.join(output_folder, csv_name)

        print(f"Convertendo {file_name} → {csv_name} ...")

        # Leitura e conversão
        try:
            table = DBF(dbf_path, load=True, encoding='latin1')
            df = pd.DataFrame(iter(table))
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"✅ Arquivo salvo em: {csv_path}")
        except Exception as e:
            print(f"⚠ Erro ao converter {file_name}: {e}")

print("\n✅ Conversão concluída! Verifique a pasta CSV.")