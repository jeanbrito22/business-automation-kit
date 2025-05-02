from checker import validate_csv_against_schema, generate_corrected_csv
import os

SCHEMA_DIR = "schema"
INPUT_DIR = "data"
OUTPUT_DIR = os.path.join(INPUT_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_batch():
    resultados = []

    for schema_file in os.listdir(SCHEMA_DIR):
        if not schema_file.startswith("tb_file_") or not schema_file.endswith(".json"):
            continue
        
        # Remove o prefixo e o sufixo do nome
        table_name = schema_file.replace("tb_file_", "").replace(".json", "")
        schema_path = os.path.join(SCHEMA_DIR, schema_file)
        input_path = os.path.join(INPUT_DIR, f"tb_file_{table_name}.csv")
        output_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")

        if not os.path.exists(input_path):
            resultados.append((table_name, "❌ CSV de entrada não encontrado"))
            continue
        try:
            validate_csv_against_schema(schema_path, input_path)
            generate_corrected_csv(schema_path, input_path, output_path)
            resultados.append((table_name, "✅ Corrigido com sucesso"))
        except Exception as e:
            resultados.append((table_name, f"❌ Erro: {str(e)}"))

    return resultados

if __name__ == "__main__":
    for tabela, status in run_batch():
        print(f"{tabela}: {status}")

