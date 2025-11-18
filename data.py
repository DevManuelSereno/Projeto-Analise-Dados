from ftplib import FTP
import os

ftp_host = "ftp.datasus.gov.br"
ftp = FTP(ftp_host)
ftp.login()

# Diretórios base do FTP
# dir_antigo = "/dissemin/publicos/SIHSUS/200801_/Dados/" Caminho para adquirir arquivos AIH
dir_antigo = "/dissemin/publicos/CNES/200508_/Dados/ST/"
# dir_novo_base = "/dissemin/publicos/SIHSUS/Novos_Sistemas/"

# Seu diretório local
local_dir = "C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/Code-Projeto/dbc-data-cnes"
os.makedirs(local_dir, exist_ok=True) # Garante que o diretório exista

anos = range(2019, 2025)
meses = range(1, 13)
uf = "BA"

# Para otimizar, vamos rastrear o diretório atual no FTP
# (Embora agora ele não vá mudar)
current_ftp_dir = ""

for ano in anos:
    for mes in meses:
        
        nome_arquivo_ftp = ""
        
        # Converte ano/mes para string
        ano_str_4 = str(ano)
        ano_str_2 = ano_str_4[-2:]
        mes_str = f"{mes:02d}"

        # --- ALTERAÇÃO SOLICITADA ---
        # O caminho do FTP agora é FIXO para a Regra 1.
        caminho_ftp = dir_antigo
        
        # A lógica do NOME do arquivo ainda muda baseada na data
        if ano < 2021 or (ano == 2021 and mes <= 7):
            # 1. Padrão de nome antigo: RDBA{YY}{MM}.dbc
            nome_arquivo_ftp = f"ST{uf.upper()}{ano_str_2}{mes_str}.dbc"
        
        elif (ano == 2021 and mes >= 8) or ano > 2021:
            # 2. Padrão de nome novo: RDBA{MM}{YY}.dbc
            nome_arquivo_ftp = f"ST{uf.upper()}{ano_str_2}{mes_str}.dbc" 

        # Caminho local onde o arquivo será salvo
        local_path = os.path.join(local_dir, nome_arquivo_ftp)

        try:
            # Otimização: Só muda o diretório no FTP se for diferente
            if caminho_ftp != current_ftp_dir:
                ftp.cwd(caminho_ftp)
                current_ftp_dir = caminho_ftp
            
            # Tenta baixar o arquivo
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {nome_arquivo_ftp}", f.write)
            print(f"Baixado: {nome_arquivo_ftp}")

        except Exception as e:
            # Se deu erro, apaga o arquivo local vazio que foi criado
            if os.path.exists(local_path):
                os.remove(local_path)
            
            if "550" in str(e):
                print(f"Arquivo não encontrado (550): {caminho_ftp}{nome_arquivo_ftp}")
            else:
                print(f"Falha ao baixar {nome_arquivo_ftp}: {e}")

ftp.quit()
print("Download Concluído!")