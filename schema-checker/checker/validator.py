# checker/validator.py
import re
import csv
from utils.file_loader import load_csv, load_schema
from utils.format_utils import convert_spark_format_to_strptime
from checker.corrector import clean_value, normalize_integer, normalize_decimal
from datetime import datetime
from pathlib import Path

def validate_value_type(value, expected_type, date_format=None, timestamp_format=None):
    value = clean_value(value)

    if expected_type == "integer":
        try:
            int(normalize_integer(value))
            return True
        except:
            return False
    elif expected_type == "decimal":
        try:
            float(normalize_decimal(value))
            return True
        except:
            return False
    elif expected_type == "string":
        return isinstance(value, str)
    elif expected_type == "date":
        try:
            fmt = convert_spark_format_to_strptime(date_format or "%d/%m/%Y")
            parsed = datetime.strptime(value, fmt)
            return parsed.strftime(fmt) == value
        except:
            return False
    elif expected_type == "timestamp":
        try:
            fmt = convert_spark_format_to_strptime(timestamp_format or "%d/%m/%Y %H:%M:%S")
            datetime.strptime(value, fmt)
            return True
        except:
            return False
    return False

def validate_csv_against_schema(schema_path, csv_path, log_path=Path("data/validation_report.log")):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]
    delimiter = input_settings["spark_read_args"].get("sep", ",")
    date_format = input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y")

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
                input_settings["spark_read_args"].get("timestampFormat") if expected_type == "timestamp" else None
            ):
                errors.append(f"Row {i+1}: Column '{source_col}' expected type '{expected_type}', got '{value}'")

    with open(log_path, "a", encoding="utf-8") as log:
        table_name = csv_path.stem.replace("tb_file_", "")
        log.write(f"\n\n=== Validação do arquivo: {table_name}.csv ===\n\n")

        if errors:
            csv_cols_set = set(csv_columns)
            schema_cols_set = set(expected_columns)
            missing_in_csv = schema_cols_set - csv_cols_set
            unexpected_in_csv = csv_cols_set - schema_cols_set
            if missing_in_csv:
                log.write(" - Columns expected in schema but missing in CSV:\n")
                for col in sorted(missing_in_csv):
                    log.write(f"    · {col}\n")
            if unexpected_in_csv:
                log.write(" - Columns found in CSV but not in schema:\n")
                for col in sorted(unexpected_in_csv):
                    log.write(f"    · {col}\n")
            for err in errors:
                log.write(f" - {err}\n")
        else:
            log.write("✅ CSV is valid against schema\n")
