import awswrangler as wr
import os
import json
from dotenv import load_dotenv

load_dotenv()

def construir_catalogo(database: str, destino_json: str):
    if os.path.exists(destino_json):
        print(f"Catálogo já existe em {destino_json}, não será recriado.")
        return

    tabelas = wr.catalog.get_tables(database=database)
    catalogo = {}

    for tabela in tabelas:
        nome = tabela["Name"]
        particoes = tabela.get("PartitionKeys", [])
        nomes_particoes = [p["Name"] for p in particoes] if particoes else []

        catalogo[nome] = {
            "partition_keys": nomes_particoes
        }

    os.makedirs(os.path.dirname(destino_json), exist_ok=True)
    with open(destino_json, "w") as f:
        json.dump(catalogo, f, indent=2)

    print(f"Catálogo salvo em {destino_json}")

# Exemplo de uso
construir_catalogo(database="silver", destino_json="config/tabelas_catalogadas/silver_tabelas.json")
