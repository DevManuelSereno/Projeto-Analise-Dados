from ftplib import FTP
import os

ftp_host = "ftp.datasus.gov.br"
ftp = FTP(ftp_host)
ftp.login()

# Diretório REAL do CNES
dir_cnes = "/dissemin/publicos/CNES/200508_/Dados/ST/"

# Diretório local
local_dir = r"C:\Users\User\Desktop\Dados\cnes\CNES-dbc"
os.makedirs(local_dir, exist_ok=True)

anos = range(2019, 2025)
meses = range(1, 13)
uf = "BA"

# Controla o diretório atual pra evitar múltiplos cwd
current_ftp_dir = ""

for ano in anos:
    for mes in meses:

        ano_str_2 = str(ano)[-2:]
        mes_str = f"{mes:02d}"

        # Nome REAL dos arquivos CNES ST (não muda nunca)
        nome_arquivo_ftp = f"ST{uf}{ano_str_2}{mes_str}.dbc"

        local_path = os.path.join(local_dir, nome_arquivo_ftp)

        try:
            if dir_cnes != current_ftp_dir:
                ftp.cwd(dir_cnes)
                current_ftp_dir = dir_cnes

            # Download
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {nome_arquivo_ftp}", f.write)

            print(f"Baixado: {nome_arquivo_ftp}")

        except Exception as e:
            if os.path.exists(local_path):
                os.remove(local_path)

            if "550" in str(e):
                print(f"Arquivo não encontrado: {dir_cnes}{nome_arquivo_ftp}")
            else:
                print(f"Erro ao baixar {nome_arquivo_ftp}: {e}")

ftp.quit()
print("Concluído!")
