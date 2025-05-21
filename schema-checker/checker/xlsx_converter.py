
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
from collections import defaultdict

# inclusao para acompanhamento de progresso na interface
def convert_single_excel_to_csv(excel_path: Path, mapping_path: Path, input_dir: Path, output_dir: Path):
    with open(mapping_path, "r", encoding="utf-8") as f:
        mappings = json.load(f)

    mappings_filtrados = [item for item in mappings if item["filename"] == excel_path.name]
    seps = {}

    dfs_por_saida = defaultdict(list)

    for item in mappings_filtrados:
        sheet_name = item["sheet_name"]
        output_csv_name = item["output_csv_name"]
        expand_target = item.get("expand_dates_to", [])

        table_name = output_csv_name.replace("tb_file_", "").replace(".csv", "")
        schema_path = Path("schema") / f"file_ingestion_{table_name}.json"

        if not schema_path.exists():
            raise FileNotFoundError(f"❌ Schema não encontrado para {table_name}: {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as sf:
            schema_data = json.load(sf)
            sep = schema_data["table_spec"][0]["input"]["spark_read_args"].get("sep", ",")
            seps[output_csv_name] = sep

        df = pd.read_excel(excel_path, sheet_name=sheet_name, engine="openpyxl")
        df = df.applymap(lambda x: str(x) if pd.notnull(x) else "")

        if expand_target:
            if len(expand_target) != 3:
                raise ValueError(f"❌ 'expand_dates_to' deve ter exatamente 3 valores (ex: ['Ano', 'Mes', 'Valor']) no arquivo {excel_path.name}")

            fixed_cols = [col for col in df.columns if not is_date_column(col)]
            value_cols = [col for col in df.columns if is_date_column(col)]

            new_rows = []
            for _, row in df.iterrows():
                for col in value_cols:
                    try:
                        parsed_date = try_parse_date(str(col))
                        new_row = row[fixed_cols].copy()
                        new_row[expand_target[0]] = parsed_date.year
                        new_row[expand_target[1]] = parsed_date.month
                        new_row[expand_target[2]] = row[col] if pd.notnull(row[col]) else 0
                        new_rows.append(new_row)
                    except Exception as e:
                        print(f"Erro ao processar coluna {col} como data: {e}")

            df = pd.DataFrame(new_rows)

        dfs_por_saida[output_csv_name].append(df)

    for output_csv_name, dfs in dfs_por_saida.items():
        final_df = pd.concat(dfs, ignore_index=True)
        output_csv = output_dir / output_csv_name
        sep = seps.get(output_csv_name, ",")
        final_df.to_csv(
            output_csv, sep=sep,
            index=False,
            encoding="utf-8-sig",
            mode='a',
            header=not output_csv.exists()
        )
        print(f"✅ Gerado: {output_csv}")

def convert_excels_to_csv(mapping_path: Path, input_dir: Path, output_dir: Path):
    for excel_path in input_dir.glob("*.xlsx"):
        convert_single_excel_to_csv(excel_path, mapping_path, input_dir, output_dir)

def is_date_column(value):
    try:
        try_parse_date(str(value))
        return True
    except:
        return False

def try_parse_date(value):
    for fmt in ["%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d 00:00:00"]:
        try:
            return datetime.strptime(value, fmt)
        except:
            continue
    raise ValueError(f"Formato de data inválido: {value}")

