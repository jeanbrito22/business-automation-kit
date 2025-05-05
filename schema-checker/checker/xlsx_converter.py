# checker/xlsx_converter.py
import json
import pandas as pd

def convert_excels_to_csv(mapping_path, xlsx_dir, output_csv_dir):
    with open(mapping_path, "r", encoding="utf-8") as f:
        mappings = json.load(f)

    for item in mappings:
        source = item["source"]
        sheet = item["sheet"]
        target = item["target"]

        source_path = xlsx_dir / source
        target_path = output_csv_dir / target

        if not source_path.exists():
            print(f"❌ Arquivo Excel não encontrado: {source_path.name}")
            continue

        try:
            df = pd.read_excel(source_path, sheet_name=sheet, dtype=str)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(target_path, index=False, encoding="utf-8-sig")
            print(f"✅ Convertido: {source_path.name} → {target_path.name}")
        except Exception as e:
            print(f"❌ Erro ao converter {source_path.name} (aba: {sheet}): {e}")
