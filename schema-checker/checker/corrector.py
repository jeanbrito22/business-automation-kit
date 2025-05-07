# checker/corrector.py
import csv
import re
from datetime import datetime
from utils.file_loader import load_csv, load_schema
from utils.format_utils import convert_spark_format_to_strptime

def clean_value(value):
    if value is None:
        return ""
    value = value.strip()
    value = " ".join(value.split())
    if value == "-":
        return ""
    return value

def normalize_timestamp(value, timestamp_format="%d/%m/%Y %H:%M:%S"):
    if not value:
        return ""

    value = clean_value(value)

    try:
        # Se já está no formato completo, retorna como está
        datetime.strptime(value, timestamp_format)
        return value
    except:
        pass

    # Casos parciais → completa com 0s
    try:
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
            return f"{value} 00:00:00"
        if re.fullmatch(r"\d{2}/\d{2}/\d{4} \d{2}", value):
            return f"{value}:00:00"
        if re.fullmatch(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}", value):
            return f"{value}:00"
    except:
        pass

    return ""

def normalize_decimal(value):
    if value is None:
        return ""
    value = clean_value(value)
    value = value.replace("R$", "").replace(" ", "")

    if "," in value:
        value = value.replace(".", "").replace(",", ".")

    if re.fullmatch(r"-?\d+(\.\d+)?", value):
        return value
    return ""

def normalize_integer(value):
    if value is None:
        return ""
    value = clean_value(value)
    value = value.replace(" ", "").replace(".", "").replace(",", "")

    if not value.isdigit():
        return ""

    return value

def validate_value_type(value, expected_type, date_format=None, timestamp_format=None):
    if expected_type == "integer":
        try:
            int(normalize_integer(value))
            return True
        except:
            return False
    elif expected_type in ("decimal", "float"):
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
            value = normalize_timestamp(value, fmt)
            parsed = datetime.strptime(value, fmt)
            return True
        except:
            return False
    return False

def generate_corrected_csv(schema_path, input_csv_path, output_csv_path):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]
    delimiter = input_settings["spark_read_args"].get("sep", ",")
    date_format = convert_spark_format_to_strptime(input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y"))
    timestamp_format = convert_spark_format_to_strptime(input_settings["spark_read_args"].get("timestampFormat", "%d/%m/%Y %H:%M:%S"))

    csv_data = load_csv(input_csv_path, schema_path, delimiter=delimiter)
    expected_columns = [col["source_column"] for col in schema]
    corrected_data = []

    for row in csv_data:
        corrected_row = {}
        for column in schema:
            source_col = column["source_column"]
            expected_type = column["type"]
            value = clean_value(row.get(source_col, ""))

            if expected_type in ("float", "decimal"):
                corrected_row[source_col] = normalize_decimal(value)
            elif expected_type == "integer":
                corrected_row[source_col] = normalize_integer(value)
            elif expected_type == "date":
                try:
                    fmt = convert_spark_format_to_strptime(input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y"))
                    parsed = datetime.strptime(value, fmt)
                    corrected_row[source_col] = parsed.strftime(fmt)
                except:
                    corrected_row[source_col] = ""
            elif expected_type == "timestamp":
                try:
                    fmt = convert_spark_format_to_strptime(input_settings["spark_read_args"].get("timestampFormat", "%d/%m/%Y %H:%M:%S"))
                    normalized = normalize_timestamp(value, fmt)
                    parsed = datetime.strptime(normalized, fmt)
                    corrected_row[source_col] = parsed.strftime(fmt)
                except:
                    corrected_row[source_col] = ""
            else:
                corrected_row[source_col] = value

        corrected_data.append(corrected_row)

    with open(output_csv_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=expected_columns, delimiter=';')
        writer.writeheader()
        writer.writerows(corrected_data)

    print(f"\n💾 CSV salvo com sucesso em: {output_csv_path}")
