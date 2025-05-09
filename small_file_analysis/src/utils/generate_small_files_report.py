import json
import os
import pandas as pd
import awswrangler as wr

# Configurações
DATABASE = "silver"
WORKGROUP = "meu-workgroup-v3"
S3_OUTPUT = "s3://bucket-athena-output/"
PARTITION_INFO_PATH = "config/tabelas_catalogadas/silver_tabelas_com_particoes.json"
OUTPUT_FOLDER = "outputs/smallfiles_report"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Carregar JSON com info de partições
with open(PARTITION_INFO_PATH, "r") as f:
    info_particoes = json.load(f)

for tabela, info in info_particoes.items():
    nome_particao = ''
    tem_particao = len(info.get("partition_keys", [])) > 0
    if tem_particao:
        nome_particao = info.get("partition_sample_values").keys()[0]

    print(f"📦 Processando tabela: {tabela} | Partição: {'Sim' if tem_particao else 'Não'}")

    try:
        # Consulta $files
        q_files = f'''
        SELECT file_path, file_format, file_size_in_bytes, record_count, partition
        FROM "{DATABASE}"."{tabela}$files"
        '''
        df_files = wr.athena.read_sql_query(
            q_files,
            database=DATABASE,
            workgroup=WORKGROUP,
            s3_output=S3_OUTPUT,
            ctas_approach=False
        )
        df_files["partition_clean"] = df_files["partition"].astype(str).str.strip()
        df_files.to_csv(f"{OUTPUT_FOLDER}/{tabela}_files.csv", index=False)
    except Exception as e:
        print(f"  ⚠️ Erro em $files da tabela {tabela}: {e}")
        continue

    # Só roda $partitions se tiver partição
    if tem_particao:
        try:
            q_parts = f'''
            SELECT partition.{nome_particao} as {nome_particao}, record_count, file_count, total_size
            FROM "{DATABASE}"."{tabela}$partitions"
            '''
            df_parts = wr.athena.read_sql_query(
                q_parts,
                database=DATABASE,
                workgroup=WORKGROUP,
                s3_output=S3_OUTPUT,
                ctas_approach=False
            )
            df_parts["partition_clean"] = df_parts["partition"].astype(str).str.strip()
            df_parts.to_csv(f"{OUTPUT_FOLDER}/{tabela}_partitions.csv", index=False)
        except Exception as e:
            print(f"  ⚠️ Erro em $partitions da tabela {tabela}: {e}")
