
import csv
import re
from datetime import datetime
from dateutil.parser import parse

from utils.file_loader import load_csv, load_schema
from utils.format_utils import convert_spark_format_to_strptime


def clean_value(value):
    if not value:
        return ""
    value = str(value).strip()
    value = " ".join(value.split())
    return "" if value == "-" else value


def normalize_integer(value):
    value = clean_value(value).replace(" ", "").replace(".", "").replace(",", "")
    return value if value.isdigit() else ""


def normalize_decimal(value):
    value = clean_value(value).replace("R$", "").replace(" ", "")
    if "," in value:
        value = value.replace(".", "").replace(",", ".")
    return value if re.fullmatch(r"-?\d+(\.\d+)?", value) else ""


def normalize_timestamp(value, timestamp_format="%d/%m/%Y %H:%M:%S"):
    value = clean_value(value)
    if not value:
        return ""
    try:
        # tenta respeitar o formato do schema
        parsed = datetime.strptime(value, timestamp_format)
    except:
        try:
            parsed = parse(value, yearfirst=True)
        except:
            return ""
    return parsed.strftime(timestamp_format)


def validate_value_type(value, expected_type, date_format=None, timestamp_format=None):
    try:
        if expected_type == "integer":
            int(normalize_integer(value))
        elif expected_type in ("decimal", "float"):
            float(normalize_decimal(value))
        elif expected_type == "string":
            return isinstance(value, str)
        elif expected_type == "date":
            fmt = convert_spark_format_to_strptime(date_format or "%d/%m/%Y")
            datetime.strptime(value, fmt)
        elif expected_type == "timestamp":
            fmt = convert_spark_format_to_strptime(timestamp_format or "%d/%m/%Y %H:%M:%S")
            normalized = normalize_timestamp(value, fmt)
            datetime.strptime(normalized, fmt)
        else:
            return False
        return True
    except:
        return False


def generate_corrected_csv(schema_path, input_csv_path, output_csv_path):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]

    delimiter = input_settings["spark_read_args"].get("sep", ",")
    date_fmt_str = input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y")
    ts_fmt_str = input_settings["spark_read_args"].get("timestampFormat", "%d/%m/%Y %H:%M:%S")
    date_fmt = convert_spark_format_to_strptime(date_fmt_str)
    ts_fmt = convert_spark_format_to_strptime(ts_fmt_str)

    csv_data = load_csv(input_csv_path, schema_path, delimiter=delimiter)
    expected_columns = [col["source_column"] for col in schema]
    corrected_data = []

    for row in csv_data:
        corrected_row = {}
        for column in schema:
            source = column["source_column"]
            tipo = column["type"]
            valor = clean_value(row.get(source, ""))

            if tipo in ("decimal", "float"):
                corrected_row[source] = normalize_decimal(valor)
            elif tipo == "integer":
                corrected_row[source] = normalize_integer(valor)
            elif tipo == "date":
                try:
                    parsed = datetime.strptime(valor, date_fmt)
                except:
                    try:
                        parsed = parse(valor, yearfirst=True)
                    except:
                        parsed = None
                corrected_row[source] = parsed.strftime(date_fmt) if parsed else ""
            elif tipo == "timestamp":
                normalized = normalize_timestamp(valor, ts_fmt)
                try:
                    parsed = datetime.strptime(normalized, ts_fmt)
                except:
                    try:
                        parsed = parse(normalized, yearfirst=True)
                    except:
                        parsed = None
                corrected_row[source] = parsed.strftime(ts_fmt) if parsed else ""
            else:
                corrected_row[source] = valor

        corrected_data.append(corrected_row)

    with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=expected_columns, delimiter=";")
        writer.writeheader()
        writer.writerows(corrected_data)

    print(f"\n💾 CSV salvo com sucesso em: {output_csv_path}")
