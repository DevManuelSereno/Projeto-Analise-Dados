import glob
import pandas as pd
import os
from sqlalchemy import create_engine

pasta_csv = r"C:\Users\User\Desktop\Dados\dadoscsv"
arquivos = glob.glob(os.path.join(pasta_csv, "*.csv"))

engine = create_engine("postgresql+psycopg2://postgres:09092008@localhost:5432/DataSusBa")

# Lista oficial das colunas da tabela
colunas_tabela = [
    'uf_zi','ano_cmpt','mes_cmpt','espec','cgc_hosp','n_aih','ident','cep','munic_res',
    'nasc','sexo','uti_mes_in','uti_mes_an','uti_mes_al','uti_mes_to','marca_uti','uti_tipo',
    'uti_dias','proc_solic','proc_realiz','val_sh','val_sp','val_tot','val_ss','val_ac',
    'val_aj','rubrica','ind_vd','car_int','idade','dias_perm','qt_diarias','qt_diarias_uti',
    'val_inicio','val_tot_est','val_ped','val_sh_fed','val_sp_fed','val_ac_fed','val_ss_fed',
    'val_aj_fed','val_inic_f','val_ped_f','cbo_solic','cbo_realiz','cid_princ','cid_secun',
    'cid_causa','car_origem','idade_anos','idade_dias','peso','qt_sessoes','instru','idade_dc',
    'sexo_dc','diag_dc','diag_sec_dc','proc_dc','coef_01','coef_02','coef_03','coef_04','coef_05',
    'coef_06','coef_07','coef_08','coef_09','coef_10','hist_comp','natureza','nat_jur','ind_uti',
    'ind_neo','ind_psic','ind_proc','ind_obst','ind_idade','ind_princ','ind_vinc','ind_filh',
    'ind_cir','ind_pedi','ind_coleta','ind_reap','ind_tipo','diag_sec_2','diag_sec_3','diag_sec_4',
    'diag_sec_5','diag_sec_6','diag_sec_7','diag_sec_8','diag_sec_9','diag_sec_10',
    'dt_solic','dt_aut','dt_realiz','dt_process','dt_pgto','dt_env','dt_ord','dt_aviso','dt_rc',
    'tipo_aih','cat_end','fil_an','justific','competencia'
]

# Colunas que são datas (AAAAMMDD -> DATE)
colunas_data = [
    'nasc','dt_solic','dt_aut','dt_realiz','dt_process',
    'dt_pgto','dt_env','dt_ord','dt_aviso','dt_rc'
]

def converter_data(valor):
    """Converte AAAAMMDD (int ou string) para datetime ou retorna None."""
    if pd.isna(valor):
        return None
    valor = str(valor).strip()

    if valor == "" or valor == "0" or len(valor) != 8:
        return None

    try:
        return pd.to_datetime(valor, format="%Y%m%d", errors="coerce")
    except:
        return None


for arquivo in arquivos:
    print(f"Importando {arquivo}...")

    try:
        df = pd.read_csv(arquivo, sep=",", encoding="latin1", low_memory=False)

        # Corrige nomes
        df.columns = [c.replace("ï»¿", "").strip().lower() for c in df.columns]

        # Garante que todas as colunas existam
        for col in colunas_tabela:
            if col not in df.columns:
                df[col] = None

        # Mantém somente as colunas da tabela
        df = df[colunas_tabela]

        # 🔥 Converte colunas de data
        for col in colunas_data:
            df[col] = df[col].apply(converter_data)

        # Importa
        df.to_sql("aih_reduzida", engine, if_exists="append", index=False)

        print(f"✅ {os.path.basename(arquivo)} importado ({len(df)} linhas).")

    except Exception as e:
        print(f"❌ Erro ao importar {arquivo}: {e}")

print("\n🎉 Importação concluída!")
