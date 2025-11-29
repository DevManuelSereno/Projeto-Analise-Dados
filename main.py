from data import baixar_cnes, baixar_aih, baixar_sp
from converter import converter_cnes, converter_aih, converter_sp
from load import carregar_csv_para_staging, carregar_todos_csvs

def menu():
    while True:
        print("\n===== MENU PRINCIPAL =====")
        print("1 - Baixar dados CNES")
        print("2 - Baixar dados AIH")
        print("3 - Converter dados CNES")
        print("4 - Converter dados AIH")
        print("5 - Upload AIH")
        print("6 - Upload CNES")
        print("7 - Baixar SP")
        print("8 - Converter SP")
        print("0 - Sair")

        opcao = input("Escolha uma opção: ").strip()

        if opcao == "1":
            print("\n🔹 Baixando dados CNES...")
            baixar_cnes()
        elif opcao == "2":
            print("\n🔹 Baixando dados AIH...")
            baixar_aih()
        elif opcao == "3":
            print("\n🔹 Convertendo dados CNES...")
            converter_cnes()
        elif opcao == "4":
            print("\n🔹 Convertendo dados AIH...")
            converter_aih()
        elif opcao == "5":
            print("\n🔹 Upload nos dados AIH...")
            carregar_todos_csvs(pasta=r"C:/Users/User/Desktop/Dados/dadoscsv",
                                    tabela_destino="staging_internacoes_ba")
        elif opcao == "6":
            print("\n🔹 Upload nos dados AIH...")
            carregar_todos_csvs(pasta=r"C:/Users/User/Desktop/Dados/cnes/CNES-csv",
                                    tabela_destino="staging_estabelecimentos_ba")
        elif opcao == "7":
            print("\n🔹 Baixando SP")
            baixar_sp(
            uf="BA", # A unidade federativa (Bahia)
            anos=range(2019, 2025), # O intervalo de anos (2019, 2020, ..., 2024)
            meses=range(1, 13), # O intervalo de meses (1 a 12)
            local_dir=r"C:\Users\User\Desktop\data\SP-dbc")    

        elif opcao == "8":
            print("\n🔹 Convertendo dados SP...")
            converter_sp()
        elif opcao == "9":
            print("\n🔹 Upload nos dados SP...")
            carregar_todos_csvs(pasta=r"C:/Users/User/Desktop/data/importando tabulação",
                                    tabela_destino="staging_municipios")                        
        elif opcao == "0":
            print("\nSaindo do programa. Até mais!")
            break
        else:
            print("⚠ Opção inválida. Digite um número de 0 a 4.")

if __name__ == "__main__":
    menu()
