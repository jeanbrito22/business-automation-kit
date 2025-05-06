from pathlib import Path
import re

def check_csv_schema_compatibility(csv_dir: Path, schema_dir: Path):
    missing = []
    for csv_file in csv_dir.glob("tb_file_*.csv"):
        table_name = csv_file.stem.replace("tb_file_", "")
        schema_path = schema_dir / f"file_ingestion_{table_name}.json"
        if not schema_path.exists():
            missing.append(csv_file.name)
    return missing

def identify_non_standard_csvs(csv_dir: Path, schema_dir: Path):
    schema_pattern = re.compile(r"file_ingestion_(.+)\.json")
    expected_csv_names = {
        f"tb_file_{schema_pattern.match(f.name).group(1)}.csv"
        for f in schema_dir.glob("file_ingestion_*.json")
        if schema_pattern.match(f.name)
    }

    actual_csv_names = {f.name for f in csv_dir.glob("*.csv")}
    non_standard_csvs = actual_csv_names - expected_csv_names
    return non_standard_csvs