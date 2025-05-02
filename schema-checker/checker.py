import re
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


def normalize_timestamp(value, timestamp_format="%d/%m/%Y %H:%M:%S"):
    if not value:
        return ""

    value = clean_value(value)

    # Casos parciais → completa com 0s
    try:
        # Só data
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
            return f"{value} 00:00:00"
        # Data + hora
        if re.fullmatch(r"\d{2}/\d{2}/\d{4} \d{2}", value):
            return f"{value}:00:00"
        # Data + hora:minuto
        if re.fullmatch(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}", value):
            return f"{value}:00"

        # Já está no formato esperado
        datetime.strptime(value, timestamp_format)
        return value
    except:
        return ""


def normalize_decimal(value):
    if value is None:
        return ""
    value = clean_value(value)
    value = value.replace("R$", "").replace(" ", "")

    if "," in value:
        # Caso esteja no formato brasileiro: 1.296,60 → 1296.60
        value = value.replace(".", "").replace(",", ".")
    # Caso contrário, preserva o ponto (padrão americano): 128.4 → 128.4

    # Valida se o valor final está no formato decimal correto
    if re.fullmatch(r"-?\d+(\.\d+)?", value):
        return value
    return ""

def validate_value_type(value, expected_type, date_format=None, timestamp_format=None):
    value = clean_value(value)

    if expected_type == "integer":
        value = clean_value(value)
        try:
            # Remove ponto se for formato como 140.000
            if "." in value:
                float_val = float(value.replace(",", "."))
                if float_val.is_integer():
                    return True
                return False
            else:
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
        value = normalize_timestamp(value, timestamp_format or "%d/%m/%Y %H:%M:%S")
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

    csv_data = load_csv(csv_path, schema_path, delimiter=delimiter)

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
            log.write("📝 CSV/Schema Header Mismatch Report:\n\n")
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
                    print(f"    · {col}\n")
            for err in errors:
                log.write(f" - {err}\n")
    else:
        with open("data/validation_report.log", "w", encoding="utf-8") as log:
            log.write("✅ CSV is valid against schema\n")

def generate_corrected_csv(schema_path, input_csv_path, output_csv_path):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]
    delimiter = input_settings["spark_read_args"].get("sep", ",")
    date_format = input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y")

    csv_data = load_csv(input_csv_path, schema_path, delimiter=delimiter)
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
                    cleaned = value.replace(" ", "").replace(",", ".")
                    if "." in cleaned:
                        parts = cleaned.split(".")
                        corrected_row[source_col] = parts[0] + parts[1]
                    else:
                        corrected_row[source_col] = str(int(cleaned))
                except:
                    corrected_row[source_col] = ""

            elif expected_type == "date":
                cleaned = clean_value(value)
                if cleaned and validate_value_type(cleaned, "date", date_format):
                    corrected_row[source_col] = cleaned
                else:
                    corrected_row[source_col] = ""  # nao mantém o valor original para análise
            elif expected_type == "timestamp":
                normalized = normalize_timestamp(value, input_settings["spark_read_args"].get("timestampFormat", "%d/%m/%Y %H:%M:%S"))
                corrected_row[source_col] = normalized if normalized else value
            else:
                corrected_row[source_col] = value

        corrected_data.append(corrected_row)

    with open(output_csv_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=expected_columns, delimiter=';')
        writer.writeheader()
        writer.writerows(corrected_data)
    print(f"\n💾 Corrected CSV saved to: {output_csv_path}")