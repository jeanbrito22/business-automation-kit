import yaml
import awswrangler as wr
from dotenv import load_dotenv
import os
from collections import defaultdict
from analyzer.metrics_calculator import classificar_particao
import pandas as pd

# Carregar variáveis de ambiente do .env
load_dotenv()

# Carregar config
def carregar_config():
    with open("config/buckets.yaml", "r") as f:
        return yaml.safe_load(f)

config = carregar_config()
bronze_conf = config["bronze"]
bucket = bronze_conf["bucket"]
prefixes = bronze_conf["prefixes"]

resultados = []

for prefix in prefixes:
    print(f"Lendo prefixo: {prefix}")
    objs = wr.s3.list_objects(f"s3://{bucket}/{prefix}")

    # Agrupar por particao extraction_date=YYYYMMDD
    particoes = defaultdict(list)
    for obj in objs:
        key = obj.split(f"{prefix}")[-1]  # remove prefixo inicial
        if "extraction_date=" in key:
            for parte in key.split("/"):
                if parte.startswith("extraction_date="):
                    particao = parte.split("=")[-1]
                    particoes[particao].append(obj)

    # Processar cada particao
    for i, (particao, arquivos) in enumerate(particoes.items()):
        if i >=3:
            break
        arquivos = arquivos[:5]
        try:
            descricoes = wr.s3.describe_objects(arquivos)
            tamanhos = [obj["Size"] for obj in descricoes]
        except Exception as e:
            print(f"Erro ao descrever objetos em {prefix} - {particao}: {e}")
            tamanhos = []

        total_bytes = sum(tamanhos)
        status_info = classificar_particao(len(tamanhos), total_bytes, tamanhos)
        resultados.append({
            "prefixo": prefix,
            "particao": particao,
            "arquivos": len(tamanhos),
            **status_info
        })

# Exportar para CSV
os.makedirs("results", exist_ok=True)
df = pd.DataFrame(resultados)
df.to_csv("results/bronze_result.csv", index=False)

# Visualizar os primeiros resultados
print(df.head())
