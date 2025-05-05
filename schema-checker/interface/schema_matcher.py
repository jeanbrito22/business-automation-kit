from pathlib import Path

def check_csv_schema_compatibility(csv_dir: Path, schema_dir: Path):
    missing = []
    for csv_file in csv_dir.glob("tb_file_*.csv"):
        table_name = csv_file.stem.replace("tb_file_", "")
        schema_path = schema_dir / f"file_ingestion_{table_name}.json"
        if not schema_path.exists():
            missing.append(csv_file.name)
    return missing
