import csv
from utils.file_loader import load_csv, load_schema
from datetime import datetime

def clean_value(value):
    if value is None:
        return ""
    value = value.strip()
    value = " ".join(value.split())  # remove múltiplos espaços consecutivos
    if value == "-":
        return ""
    return value

def normalize_decimal(value):
    if value is None:
        return ""
    return value.replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")

def validate_value_type(value, expected_type, date_format=None, timestamp_format=None):
    value = clean_value(value)

    if expected_type == "integer":
        if any(c in value for c in ["R$", ",", "."]):
            return False
        try:
            int(value)
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
            datetime.strptime(value, date_format or "%d/%m/%Y")
            return True
        except:
            return False
    elif expected_type == "timestamp":
        try:
            datetime.strptime(value, timestamp_format or "%d/%m/%Y %H:%M:%S")
            return True
        except:
            return False
    return False

def validate_csv_against_schema(schema_path, csv_path):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]
    delimiter = input_settings["spark_read_args"].get("sep", ",")
    header = input_settings["spark_read_args"].get("header", True)
    date_format = input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y")

    csv_data = load_csv(csv_path, delimiter=delimiter)

    expected_columns = [col["source_column"] for col in schema]
    csv_columns = csv_data[0].keys()

    errors = []

    # Verifica colunas extras
    for col in csv_columns:
        if col not in expected_columns:
            errors.append(f"Header error: Unexpected column '{col}' not in schema")

    # Verifica ordem
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

    if errors:
        with open("data/validation_report.log", "w", encoding="utf-8") as log:
        # Gera relatório de diferenças entre o cabeçalho do CSV e o schema
            log.write("📝 CSV/Schema Header Mismatch Report:")
            csv_cols_set = set(csv_columns)
            schema_cols_set = set(expected_columns)
            missing_in_csv = schema_cols_set - csv_cols_set
            unexpected_in_csv = csv_cols_set - schema_cols_set
            if missing_in_csv:
                log.write(" - Columns expected in schema but missing in CSV:")
                for col in sorted(missing_in_csv):
                    log.write(f"    · {col}")
            if unexpected_in_csv:
                log.write(" - Columns found in CSV but not in schema:")
                for col in sorted(unexpected_in_csv):
                    print(f"    · {col}")
            for err in errors:
                log.write(f" - {err}")
    else:
        with open("data/validation_report.log", "w", encoding="utf-8") as log:
            log.write(" ✅ CSV is valid against schema")

def generate_corrected_csv(schema_path, input_csv_path, output_csv_path):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]
    delimiter = input_settings["spark_read_args"].get("sep", ",")
    date_format = input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y")

    csv_data = load_csv(input_csv_path, delimiter=delimiter)
    expected_columns = [col["source_column"] for col in schema]

    corrected_data = []
    for row in csv_data:
        corrected_row = {}
        for column in schema:
            source_col = column["source_column"]
            expected_type = column["type"]
            value = clean_value(row.get(source_col, ""))

            if expected_type == "decimal":
                try:
                    normalized = normalize_decimal(value)
                    corrected_row[source_col] = str(float(normalized)) if normalized else ""
                except:
                    corrected_row[source_col] = ""
            elif expected_type == "integer":
                try:
                    corrected_row[source_col] = str(int(value.replace(" ", ""))) if value else ""
                except:
                    corrected_row[source_col] = ""
            elif expected_type == "date":
                try:
                    datetime.strptime(value, date_format)
                    corrected_row[source_col] = value
                except:
                    corrected_row[source_col] = ""
            else:
                corrected_row[source_col] = value

        corrected_data.append(corrected_row)

    with open(output_csv_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=expected_columns, delimiter=';')
        writer.writeheader()
        writer.writerows(corrected_data)
    print(f"\n💾 Corrected CSV saved to: {output_csv_path}")