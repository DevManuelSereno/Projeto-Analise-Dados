import os
import pandas as pd
from dbfread import DBF
from dbctodbf import DBCDecompress


class DBCConverter:
    """
    Classe para converter arquivos .DBC em CSV.
    """

    def __init__(self, input_folder: str, output_folder: str):
        self.input_folder = input_folder
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)
        self.decompresser = DBCDecompress()

    # --------------------------
    # Localizar arquivos .DBC
    # --------------------------
    def listar_dbc_files(self):
        return [f for f in os.listdir(self.input_folder) if f.lower().endswith(".dbc")]

    # --------------------------
    # Converter um único arquivo
    # --------------------------
    def converter_arquivo(self, file_name: str):
        dbc_path = os.path.join(self.input_folder, file_name)
        base_name = os.path.splitext(file_name)[0]

        try:
            # DBC → DBF
            dbf_path = os.path.join(self.output_folder, f"{base_name}.dbf")
            self.decompresser.decompressFile(dbc_path, dbf_path)

            # DBF → DataFrame
            table = DBF(dbf_path, encoding="latin1")
            df = pd.DataFrame(iter(table))

            # Salvar CSV
            csv_path = os.path.join(self.output_folder, f"{base_name}.csv")
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")

            print(f"✅ Sucesso: {file_name} → {csv_path}")
            return csv_path

        except Exception as e:
            print(f"❌ Erro ao converter {file_name}: {e}")
            return None

    # --------------------------
    # Converter todos os arquivos
    # --------------------------
    def converter_todos(self):
        dbc_files = self.listar_dbc_files()
        if not dbc_files:
            print("⚠ Nenhum arquivo .DBC encontrado.")
            return []

        print(f"📦 {len(dbc_files)} arquivo(s) .DBC encontrado(s). Iniciando conversão...\n")
        converted_files = []

        for file_name in dbc_files:
            print(f"🔸 Convertendo {file_name}...")
            csv_path = self.converter_arquivo(file_name)
            if csv_path:
                converted_files.append(csv_path)

        print("\n🎉 Conversão finalizada com sucesso!")
        return converted_files

    # --------------------------
    # Limpar arquivos DBF temporários
    # --------------------------
    def limpar_dbf_temporarios(self):
        removed_files = []
        for f in os.listdir(self.output_folder):
            if f.lower().endswith(".dbf"):
                try:
                    os.remove(os.path.join(self.output_folder, f))
                    removed_files.append(f)
                    print(f"🧹 Arquivo temporário removido: {f}")
                except Exception as e:
                    print(f"⚠ Erro ao remover {f}: {e}")
        return removed_files


# ==========================
# Função específica para CNES
# ==========================
def converter_cnes():
    input_folder = r"C:\Users\User\Desktop\Dados\cnes\CNES-dbc"
    output_folder = r"C:\Users\User\Desktop\Dados\cnes\CNES-csv"

    converter = DBCConverter(input_folder, output_folder)
    converter.converter_todos()
    converter.limpar_dbf_temporarios()


# ==========================
# Função específica para AIH
# ==========================
def converter_aih():
    input_folder = r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/Code-Projeto/dbc-data-aih"
    output_folder = r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/Code-Projeto/csv-data-aih"

    converter = DBCConverter(input_folder, output_folder)
    converter.converter_todos()
    converter.limpar_dbf_temporarios()


def converter_sp():
    input_folder = r"C:/Users/User/Desktop/data/SP-dbc"   # pasta onde estão os .dbc de SP
    output_folder = r"C:/Users/User/Desktop/data/SP-csv"  # pasta onde serão salvos os csv

    converter = DBCConverter(input_folder, output_folder)
    converter.converter_todos()
    converter.limpar_dbf_temporarios()    


# ==========================
# Exemplo de uso
# ==========================
# if __name__ == "__main__":
#     print("🔹 Convertendo CNES...")
#     converter_cnes()
    # print("\n🔹 Convertendo AIH...")
    # converter_aih()
