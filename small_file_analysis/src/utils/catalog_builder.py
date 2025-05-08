import awswrangler as wr
from dotenv import load_dotenv
import os
import json

# Carregar variáveis de ambiente do .env
load_dotenv()

# Define database e caminho de destino
DATABASE = "silver"
DESTINO_JSON = "config/tabelas_catalogadas/silver_tabelas.json"

# Não sobrescreve se já existir
if os.path.exists(DESTINO_JSON):
    print(f"Catálogo já existe em {DESTINO_JSON}, não será recriado.")
    exit()

# Obter nomes das tabelas
print(f"Buscando tabelas do Glue no banco '{DATABASE}'...")
tabelas = wr.catalog.get_tables(database=DATABASE)
nomes = sorted([t["Name"] for t in tabelas])

# Salvar em JSON
os.makedirs(os.path.dirname(DESTINO_JSON), exist_ok=True)
with open(DESTINO_JSON, "w") as f:
    json.dump(nomes, f, indent=2)

print(f"Tabelas salvas em {DESTINO_JSON}:")
print(nomes)
