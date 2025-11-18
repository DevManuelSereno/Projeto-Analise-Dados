from ftplib import FTP, error_perm
import os
import time
import tempfile

ftp_host = "ftp.datasus.gov.br"
ftp = FTP(ftp_host, timeout=60)
ftp.login()
ftp.set_pasv(True)

# Diretório FTP (ST)
dir_cnes = "/dissemin/publicos/CNES/200508_/Dados/ST/"

# Diretório local
local_dir = r"C:/Users/GAMER/OneDrive/Documentos/Faculdade/Projeto-DADOS/Code-Projeto/dbc-data-cnes"
os.makedirs(local_dir, exist_ok=True)

anos = range(2019, 2025)
meses = range(1, 13)
uf = "BA"

# parâmetros de retry
MAX_RETRIES = 5
SLEEP_BETWEEN_RETRIES = 3  # segundos
BLOCKSIZE = 1024 * 64  # tamanho do chunk para retrbinary

# entra no diretório FTP uma vez
ftp.cwd(dir_cnes)

def download_safe(ftp, remote_name, local_path):
    """
    Faz download seguro com retry e validação de tamanho (quando disponível).
    Salva primeiro em arquivo temporário e faz rename ao final.
    Retorna True se sucesso, False se falhou.
    """
    # tenta descobrir tamanho remoto (pode levantar error_perm se não suportado)
    remote_size = None
    try:
        remote_size = ftp.size(remote_name)
    except Exception:
        remote_size = None

    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            # arquivo temporário no mesmo diretório (garante atomicidade de replace)
            fd, tmp_path = tempfile.mkstemp(prefix="._tmp_", dir=os.path.dirname(local_path))
            os.close(fd)
            with open(tmp_path, "wb") as f:
                def callback(data):
                    f.write(data)
                # iniciar download
                ftp.retrbinary(f"RETR {remote_name}", callback, blocksize=BLOCKSIZE)
            # se remote_size conhecido, compare
            downloaded_size = os.path.getsize(tmp_path)
            if remote_size is not None and downloaded_size != remote_size:
                print(f"⚠ Tamanho mismatch (tentativa {attempt}/{MAX_RETRIES}) para {remote_name}: esperado {remote_size}, baixado {downloaded_size}. Retentando...")
                os.remove(tmp_path)
                time.sleep(SLEEP_BETWEEN_RETRIES)
                continue
            # Tudo ok: move para destino final (substitui se existir)
            os.replace(tmp_path, local_path)
            return True

        except error_perm as e:
            # Erros de permissão (ex.: 550 file not found)
            # remove tmp se existir
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            # re-raise para tratamento externo
            raise

        except Exception as e:
            # limpeza do tmp e retry
            print(f"⚠ Erro no download de {remote_name} (tentativa {attempt}/{MAX_RETRIES}): {e}")
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            time.sleep(SLEEP_BETWEEN_RETRIES)

    # se chegou aqui, todas as tentativas falharam
    return False

# loop principal (mantive sua lógica de nomes)
for ano in anos:
    for mes in meses:
        ano_str_4 = str(ano)
        ano_str_2 = ano_str_4[-2:]
        mes_str = f"{mes:02d}"
        nome_arquivo_ftp = f"ST{uf.upper()}{ano_str_2}{mes_str}.dbc"
        local_path = os.path.join(local_dir, nome_arquivo_ftp)

        try:
            # check existe local e tamanho coincide com o remoto (se remoto existe)
            remote_size = None
            try:
                remote_size = ftp.size(nome_arquivo_ftp)
            except Exception:
                remote_size = None

            if os.path.exists(local_path) and remote_size is not None:
                local_size = os.path.getsize(local_path)
                if local_size == remote_size:
                    print(f"Pulado (já existe e tamanho ok): {nome_arquivo_ftp}")
                    continue
                else:
                    print(f"Arquivo local existe mas tamanho difere (local={local_size} remote={remote_size}). Rebaixando...")

            # tenta baixar com método seguro
            ok = download_safe(ftp, nome_arquivo_ftp, local_path)
            if ok:
                print(f"Baixado com sucesso: {nome_arquivo_ftp}")
            else:
                print(f"Falha ao baixar após {MAX_RETRIES} tentativas: {nome_arquivo_ftp}")

        except Exception as e:
            # tratamento de 550 e outros
            msg = str(e)
            if "550" in msg or isinstance(e, error_perm):
                print(f"Arquivo não encontrado (550): {dir_cnes}{nome_arquivo_ftp}")
            else:
                print(f"Falha ao baixar {nome_arquivo_ftp}: {e}")

ftp.quit()
print("Download Concluído!")
