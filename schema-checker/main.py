# main.py
import argparse
from pathlib import Path
from checker.validator import validate_csv_against_schema
from checker.corrector import generate_corrected_csv

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_DIR = BASE_DIR / "schema"
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def run_batch(mode="validate"):
    resultados = []

    for schema_file in SCHEMA_DIR.glob("tb_file_*.json"):
        table_name = schema_file.stem.replace("tb_file_", "")
        input_path = DATA_DIR / f"tb_file_{table_name}.csv"
        output_path = OUTPUT_DIR / f"{table_name}.csv"

        if not input_path.exists():
            resultados.append((table_name, "❌ CSV de entrada não encontrado"))
            continue

        try:
            validate_csv_against_schema(schema_file, input_path)
            if mode == "correct":
                generate_corrected_csv(schema_file, input_path, output_path)
            resultados.append((table_name, "✅ Processado com sucesso"))
        except Exception as e:
            resultados.append((table_name, f"❌ Erro: {str(e)}"))

    return resultados

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validador e Corretor de CSVs baseado em schema JSON.")
    parser.add_argument("--mode", choices=["validate", "correct"], default="validate",
                        help="Modo de execução: apenas validar ou validar + corrigir")
    args = parser.parse_args()

    print(f"\n🛠️  Modo de execução: {args.mode}\n")
    for tabela, status in run_batch(args.mode):
        print(f"{tabela}: {status}")
