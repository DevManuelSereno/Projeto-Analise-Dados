from data import baixar_cnes, baixar_aih
from converter import converter_cnes, converter_aih

def menu():
    while True:
        print("\n===== MENU PRINCIPAL =====")
        print("1 - Baixar dados CNES")
        print("2 - Baixar dados AIH")
        print("3 - Converter dados CNES")
        print("4 - Converter dados AIH")
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
        elif opcao == "0":
            print("\nSaindo do programa. Até mais!")
            break
        else:
            print("⚠ Opção inválida. Digite um número de 0 a 4.")

if __name__ == "__main__":
    menu()
