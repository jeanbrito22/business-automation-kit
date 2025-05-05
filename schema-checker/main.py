# main.py
import argparse
from pathlib import Path
from checker.validator import validate_csv_against_schema
from checker.corrector import generate_corrected_csv
from checker.xlsx_converter import convert_excels_to_csv  # novo import

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_DIR = BASE_DIR / "schema"
DATA_DIR = BASE_DIR / "data"
INPUT_XLSX_DIR = DATA_DIR / "inputs" / "xlsx"
INPUT_CSV_DIR = DATA_DIR / "inputs" / "csv"
OUTPUT_DIR = DATA_DIR / "outputs"
LOG_DIR = DATA_DIR / "logs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


def run_batch(mode="validate"):
    resultados = []

    if mode.startswith("convert"):
        convert_excels_to_csv(INPUT_XLSX_DIR / "excel_mapping.json", INPUT_XLSX_DIR, INPUT_CSV_DIR)
        resultados.append(("Conversão", "✅ Arquivos Excel convertidos com sucesso"))
        post_mode = "correct" if mode == "convert_correct" else "validate"
        mode = post_mode

    for idx, schema_file in enumerate(SCHEMA_DIR.glob("file_ingestion_*.json")):
        table_name = schema_file.stem.replace("file_ingestion_", "")
        input_path = INPUT_CSV_DIR / f"tb_file_{table_name}.csv"
        output_path = OUTPUT_DIR / f"{table_name}.csv"
        log_path = LOG_DIR / "validation_report.log"
        if not input_path.exists():
            resultados.append((table_name, "❌ CSV de entrada não encontrado"))
            continue

        try:
            validate_csv_against_schema(schema_file, input_path, append=(idx > 0), log_path=log_path)
            if mode == "correct":
                generate_corrected_csv(schema_file, input_path, output_path)
            resultados.append((table_name, "✅ Processado com sucesso"))
        except Exception as e:
            resultados.append((table_name, f"❌ Erro: {str(e)}"))

    return resultados


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validador e Corretor de CSVs baseado em schema JSON.")
    parser.add_argument("--mode", choices=["validate", "correct", "convert", "convert_correct"], default="validate",
                        help="Modo de execução: validar, corrigir, converter excel ou converter e corrigir")
    args = parser.parse_args()

    print(f"\n🛠️  Modo de execução: {args.mode}\n")
    for tabela, status in run_batch(args.mode):
        print(f"{tabela}: {status}")
