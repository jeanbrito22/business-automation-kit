import awswrangler as wr
from dotenv import load_dotenv
import os
import json
import boto3
import pandas as pd

# Carregar variáveis de ambiente do .env
load_dotenv()

# Define database e caminho de destino
DATABASE = "silver"
ENTRADA_JSON = "config/tabelas_catalogadas/silver_tabelas.json"
DESTINO_JSON = "config/tabelas_catalogadas/silver_tabelas_com_particoes.json"

# Verificar se entrada existe
if not os.path.exists(ENTRADA_JSON):
    raise FileNotFoundError(f"Arquivo {ENTRADA_JSON} não encontrado.")

# Carregar nomes das tabelas
with open(ENTRADA_JSON, "r") as f:
    tabelas = json.load(f)

resultado = {}

for tabela in tabelas:
    print(f"Verificando particao da tabela: {tabela}")
    try:
        df = wr.athena.read_sql_query(
            f'SELECT * FROM "{DATABASE}"."{tabela}$partitions" LIMIT 1',
            database=DATABASE
        )
        if not df.empty:
            part_cols = list(df.columns)
            resultado[tabela] = {"partition_keys": part_cols}
        else:
            resultado[tabela] = {"partition_keys": []}
    except Exception as e:
        print(f"  -> Não possui particao ou falha: {e}")
        resultado[tabela] = {"partition_keys": []}

# Salvar resultado
os.makedirs(os.path.dirname(DESTINO_JSON), exist_ok=True)
with open(DESTINO_JSON, "w") as f:
    json.dump(resultado, f, indent=2)

print(f"Catálogo de particoes salvo em {DESTINO_JSON}")
