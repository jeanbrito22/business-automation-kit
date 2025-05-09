import awswrangler as wr
from dotenv import load_dotenv
import os
import json
import time
import pandas as pd

# Carregar variáveis de ambiente do .env
load_dotenv()

DATABASE = "silver"
ENTRADA_JSON = "config/tabelas_catalogadas/silver_tabelas.json"
DESTINO_JSON = "config/tabelas_catalogadas/silver_tabelas_com_particoes.json"
WORKGROUP = "meu-workgroup-v3"
S3_OUTPUT = "s3://meu-bucket-athena-results/"  # ajuste conforme necessário

# Garantir que o JSON de saída já existente seja carregado para evitar retrabalho
if os.path.exists(DESTINO_JSON):
    with open(DESTINO_JSON, "r") as f:
        resultado = json.load(f)
else:
    resultado = {}

# Carregar lista de tabelas
with open(ENTRADA_JSON, "r") as f:
    tabelas = json.load(f)

for tabela in tabelas:
    if tabela in resultado:
        print(f"✅ Tabela já processada: {tabela}")
        continue

    print(f"🔍 Verificando partições da tabela: {tabela}")
    try:
        query = f'SELECT * FROM "{DATABASE}"."{tabela}$partitions" LIMIT 1'
        df = wr.athena.read_sql_query(
            sql=query,
            database=DATABASE,
            workgroup=WORKGROUP,
            s3_output=S3_OUTPUT,
            ctas_approach=False
        )

        if not df.empty:
            partition_keys = list(df.columns)

            # Tentar extrair o conteúdo do campo "partition"
            partition_values = {}
            if "partition" in df.columns:
                val = df["partition"].iloc[0]
                # Esperado: string como "extraction_date=20250412"
                for part in val.split("/"):
                    if "=" in part:
                        k, v = part.split("=")
                        partition_values[k] = v

            resultado[tabela] = {
                "partition_keys": partition_keys,
                "partition_sample_values": partition_values
            }

        else:
            resultado[tabela] = {
                "partition_keys": [],
                "partition_sample_values": {}
            }

    except Exception as e:
        print(f"⚠️  Erro na tabela {tabela}: {e}")
        resultado[tabela] = {
            "partition_keys": [],
            "partition_sample_values": {}
        }

    # Salvar progresso parcial a cada tabela
    os.makedirs(os.path.dirname(DESTINO_JSON), exist_ok=True)
    with open(DESTINO_JSON, "w") as f:
        json.dump(resultado, f, indent=2)

    time.sleep(0.5)  # evitar throttling

print(f"✅ Catálogo atualizado salvo em: {DESTINO_JSON}")
