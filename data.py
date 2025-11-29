from ftplib import FTP, error_perm
import os
import tempfile
import time


# ==========================
# Classe genérica FTPDownloader
# ==========================
class FTPDownloader:
    def __init__(
        self,
        ftp_host,
        ftp_dir,
        local_dir,
        uf=None,
        max_retries=5,
        sleep_between_retries=3,
        blocksize=1024 * 64,
        pattern=None  # função: (ano, mes, uf) -> nome_arquivo
    ):
        self.ftp_host = ftp_host
        self.ftp_dir = ftp_dir
        self.local_dir = local_dir
        self.uf = uf
        self.max_retries = max_retries
        self.sleep_between_retries = sleep_between_retries
        self.blocksize = blocksize
        self.pattern = pattern
        self.ftp = None

        os.makedirs(self.local_dir, exist_ok=True)

    # --------------------------
    # Conexão FTP
    # --------------------------
    def conectar(self, timeout=60, pasv=True):
        self.ftp = FTP(self.ftp_host, timeout=timeout)
        self.ftp.login()
        self.ftp.set_pasv(pasv)
        self.ftp.cwd(self.ftp_dir)
        print(f"Conectado ao FTP: {self.ftp_host}, diretório: {self.ftp_dir}")

    # --------------------------
    # Gerar nome de arquivo
    # --------------------------
    def gerar_nome_arquivo(self, ano, mes):
        if self.pattern:
            return self.pattern(ano, mes, self.uf)
        # padrão CNES
        ano2 = str(ano)[-2:]
        mes2 = f"{mes:02d}"
        return f"ST{self.uf.upper()}{ano2}{mes2}.dbc"

    # --------------------------
    # Download seguro de um arquivo
    # --------------------------
    def download_arquivo(self, nome_arquivo):
        if self.ftp is None:
            raise RuntimeError("FTP não está conectado. Chame conectar() primeiro.")

        local_path = os.path.join(self.local_dir, nome_arquivo)

        # tenta obter tamanho remoto
        try:
            remote_size = self.ftp.size(nome_arquivo)
        except Exception:
            remote_size = None

        # verifica arquivo local
        if os.path.exists(local_path) and remote_size is not None:
            local_size = os.path.getsize(local_path)
            if local_size == remote_size:
                print(f"Pulado (já existe e tamanho ok): {nome_arquivo}")
                return True
            else:
                print(f"Tamanho diferente (local={local_size}, remote={remote_size}), rebaixando...")

        attempt = 0
        while attempt < self.max_retries:
            attempt += 1
            tmp_path = None
            try:
                fd, tmp_path = tempfile.mkstemp(prefix="._tmp_", dir=self.local_dir)
                os.close(fd)

                with open(tmp_path, "wb") as f:
                    def callback(data):
                        f.write(data)
                    self.ftp.retrbinary(f"RETR {nome_arquivo}", callback, blocksize=self.blocksize)

                # validação tamanho
                if remote_size is not None:
                    downloaded_size = os.path.getsize(tmp_path)
                    if downloaded_size != remote_size:
                        print(f"⚠ Tamanho mismatch (tentativa {attempt}/{self.max_retries}): "
                              f"{nome_arquivo} (esperado {remote_size}, baixado {downloaded_size})")
                        os.remove(tmp_path)
                        time.sleep(self.sleep_between_retries)
                        continue

                os.replace(tmp_path, local_path)
                print(f"Baixado com sucesso: {nome_arquivo}")
                return True

            except error_perm as e:
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)
                if "550" in str(e):
                    print(f"Arquivo não encontrado (550): {self.ftp_dir}{nome_arquivo}")
                    return False
                raise

            except Exception as e:
                print(f"⚠ Erro no download de {nome_arquivo} (tentativa {attempt}/{self.max_retries}): {e}")
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)
                time.sleep(self.sleep_between_retries)

        print(f"Falha após {self.max_retries} tentativas: {nome_arquivo}")
        return False

    # --------------------------
    # Baixar múltiplos arquivos
    # --------------------------
    def baixar_arquivos(self, nomes_arquivos):
        resultados = {}
        for nome in nomes_arquivos:
            resultados[nome] = self.download_arquivo(nome)
        return resultados

    # --------------------------
    # Fechar conexão FTP
    # --------------------------
    def fechar(self):
        if self.ftp:
            self.ftp.quit()
            self.ftp = None
            print("Conexão FTP encerrada.")


# ==========================
# Função específica para CNES
# ==========================
def baixar_cnes(uf, anos, meses, local_dir):
    downloader = FTPDownloader(
        ftp_host="ftp.datasus.gov.br",
        ftp_dir="/dissemin/publicos/CNES/200508_/Dados/ST/",
        local_dir=local_dir,
        uf=uf
    )
    downloader.conectar()
    arquivos = [downloader.gerar_nome_arquivo(ano, mes) for ano in anos for mes in meses]
    downloader.baixar_arquivos(arquivos)
    print("Arquivos baixados com sucesso!")
    downloader.fechar()


# ==========================
# Função específica para AIH
# ==========================
def baixar_aih(uf, anos, meses, local_dir):
    # padrão de nome para AIH
    def nome_aih(ano, mes, uf):
        ano2 = str(ano)[-2:]
        mes2 = f"{mes:02d}"
        return f"RD{uf.upper()}{ano2}{mes2}.dbc"

    downloader = FTPDownloader(
        ftp_host="ftp.datasus.gov.br",
        ftp_dir="/dissemin/publicos/SIHSUS/200801_/Dados/",
        local_dir=local_dir,
        uf=uf,
        pattern=nome_aih
    )
    downloader.conectar()
    arquivos = [downloader.gerar_nome_arquivo(ano, mes) for ano in anos for mes in meses]
    downloader.baixar_arquivos(arquivos)
    print("Arquivos baixados com sucesso!")
    downloader.fechar()


def baixar_sp(uf, anos, meses, local_dir):
    # padrão de nome para SP no SIH/SUS
    def nome_sp(ano, mes, uf):
        ano2 = str(ano)[-2:]
        mes2 = f"{mes:02d}"
        return f"SP{uf.upper()}{ano2}{mes2}.dbc"

    downloader = FTPDownloader(
        ftp_host="ftp.datasus.gov.br",
        ftp_dir="/dissemin/publicos/SIHSUS/200801_/Dados/",
        local_dir=local_dir,
        uf=uf,
        pattern=nome_sp
    )

    downloader.conectar()
    arquivos = [downloader.gerar_nome_arquivo(ano, mes) for ano in anos for mes in meses]
    downloader.baixar_arquivos(arquivos)
    print("Arquivos SP baixados com sucesso!")
    downloader.fechar()



   
    
# ==========================
# Exemplo de uso
# # ==========================
# if __name__ == "__main__":
#     # Baixar CNES
#     baixar_cnes(
#         uf="BA",
#         anos=range(2019, 2025),
#         meses=range(1, 13),
#         local_dir=r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/Code-Projeto/dbc-data-cnes" # Mudar caminho quando necessário
#     )

# #     # Baixar AIH
#     baixar_aih(
#         uf="BA",
#         anos=range(2019, 2025),
#         meses=range(1, 13),
#         local_dir=r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/Code-Projeto/dbc-data-aih" # Mudar caminho quando necessário
#     )