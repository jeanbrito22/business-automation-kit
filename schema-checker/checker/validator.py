
from datetime import datetime
from pathlib import Path
from collections import Counter

from utils.file_loader import load_csv, load_schema
from utils.format_utils import convert_spark_format_to_strptime
from checker.corrector import clean_value, normalize_integer, normalize_decimal


def validate_value_type(value, expected_type, date_format=None, timestamp_format=None):
    value = clean_value(value)

    if expected_type == "integer":
        return normalize_integer(value) != ""

    if expected_type in ("float", "decimal"):
        try:
            float(normalize_decimal(value))
            return True
        except:
            return False

    if expected_type == "string":
        return isinstance(value, str)

    if expected_type == "date":
        try:
            fmt = convert_spark_format_to_strptime(date_format or "%d/%m/%Y")
            return datetime.strptime(value, fmt).strftime(fmt) == value
        except:
            return False

    if expected_type == "timestamp":
        try:
            fmt = convert_spark_format_to_strptime(timestamp_format or "%d/%m/%Y %H:%M:%S")
            datetime.strptime(value, fmt)
            return True
        except:
            return False

    return False


def validate_csv_against_schema(schema_path, csv_path, log_path=Path("data/logs/validation_report.log"), append=False):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]

    delimiter = input_settings["spark_read_args"].get("sep", ",")
    date_format = input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y")
    timestamp_format = input_settings["spark_read_args"].get("timestampFormat", "%d/%m/%Y %H:%M:%S")

    csv_data = load_csv(csv_path, schema_path, delimiter=delimiter)
    expected_columns = [col["source_column"] for col in schema]
    csv_columns = csv_data[0].keys()

    error_counter = Counter()

    # Checagem de cabeçalho
    csv_cols_set = set(csv_columns)
    schema_cols_set = set(expected_columns)
    for col in csv_columns:
        if col not in expected_columns:
            error_counter[f"Coluna desconhecida '{col}' não está no schema JSON"] += 1

    if list(csv_columns) != expected_columns:
        error_counter["A ordem da coluna não segue o schema JSON"] += 1

    # Checagem linha a linha
    for i, row in enumerate(csv_data):
        for column in schema:
            source_col = column["source_column"]
            expected_type = column["type"]
            value = clean_value(row.get(source_col, ""))

            if value and not validate_value_type(
                value,
                expected_type,
                date_format if expected_type == "date" else None,
                timestamp_format if expected_type == "timestamp" else None
            ):
                msg = f"Coluna '{source_col}' tipo esperado '{expected_type}', tipo encontrado '{value}'"
                error_counter[msg] += 1

    # Escrita no log
    with open(log_path, "a" if append else "w", encoding="utf-8") as log:
        table_name = csv_path.stem.replace("tb_file_", "")
        log.write(f"\n\n=== Validação do arquivo: {table_name}.csv ===\n\n")

        if error_counter:
            missing_in_csv = schema_cols_set - csv_cols_set
            unexpected_in_csv = csv_cols_set - schema_cols_set

            if missing_in_csv:
                log.write(" - Colunas esperadas no schema JSON mas faltando no CSV:\n")
                for col in sorted(missing_in_csv):
                    log.write(f"    · {col}\n")

            if unexpected_in_csv:
                log.write(" - Colunas encontradas no CSV mas não no schema JSON:\n")
                for col in sorted(unexpected_in_csv):
                    log.write(f"    · {col}\n")

            for msg, count in error_counter.items():
                prefix = f"{count}x: " if count > 1 else ""
                log.write(f" - {prefix}{msg}\n")
        else:
            log.write("✅ O CSV é validado quando comparado ao Schema\n")
