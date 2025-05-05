# checker/validator.py
import re
from datetime import datetime
from utils.file_loader import load_csv, load_schema
from utils.format_utils import convert_spark_format_to_strptime
from checker.corrector import clean_value, validate_value_type

def validate_csv_against_schema(schema_path, csv_path):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]
    delimiter = input_settings["spark_read_args"].get("sep", ",")
    header = input_settings["spark_read_args"].get("header", True)
    date_format = convert_spark_format_to_strptime(input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y"))
    timestamp_format = convert_spark_format_to_strptime(input_settings["spark_read_args"].get("timestampFormat", "%d/%m/%Y %H:%M:%S"))

    csv_data = load_csv(csv_path, schema_path, delimiter=delimiter)
    expected_columns = [col["source_column"] for col in schema]
    csv_columns = csv_data[0].keys()

    errors = []

    for col in csv_columns:
        if col not in expected_columns:
            errors.append(f"Header error: Unexpected column '{col}' not in schema")

    if list(csv_columns) != expected_columns:
        errors.append("Header error: Column order does not match schema")

    for i, row in enumerate(csv_data):
        for column in schema:
            source_col = column["source_column"]
            expected_type = column["type"]
            value = row.get(source_col)
            value = clean_value(value)

            if value.strip() != "" and not validate_value_type(
                value,
                expected_type,
                date_format if expected_type == "date" else None,
                timestamp_format if expected_type == "timestamp" else None
            ):
                errors.append(f"Row {i+1}: Column '{source_col}' expected type '{expected_type}', got '{value}'")

    with open("data/validation_report.log", "a", encoding="utf-8") as log:
        table_name = csv_path.stem.replace("tb_file_", "")
        log.write(f"\n\n=== Validação do arquivo: {table_name}.csv ===\n\n")
        if errors:
            log.write("\U0001F4DD CSV/Schema Header Mismatch Report:\n\n")
            for err in errors:
                log.write(f" - {err}\n")
        else:
            log.write("\u2705 CSV is valid against schema\n")
