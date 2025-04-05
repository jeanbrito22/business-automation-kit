import csv
import json
from utils.file_loader import load_csv, load_schema
from datetime import datetime

def clean_value(value):
    if value is None:
        return ""
    value = value.strip()
    if value == "-":
        return ""
    return value

def normalize_decimal(value):
    return value.replace("R$", "").replace(" ", "").replace(",", ".")

def validate_value_type(value, expected_type, date_format=None):
    value = clean_value(value)

    if expected_type == "integer":
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

    errors = []

    for i, row in enumerate(csv_data):
        for column in schema:
            source_col = column["source_column"]
            expected_type = column["type"]
            value = row.get(source_col)
            value = clean_value(value)

            if value == "":
                errors.append(f"Row {i+1}: Missing value for '{source_col}'")
            elif not validate_value_type(value, expected_type, date_format):
                errors.append(f"Row {i+1}: Column '{source_col}' expected type '{expected_type}', got '{value}'")

    if errors:
        print("\n🚨 Validation errors found:")
        for err in errors:
            print(" -", err)
    else:
        print("\n✅ CSV is valid against schema")

def generate_corrected_csv(schema_path, input_csv_path, output_csv_path):
    schema_data = load_schema(schema_path)
    table_spec = schema_data["table_spec"][0]
    schema = table_spec["schema"]
    input_settings = table_spec["input"]
    delimiter = input_settings["spark_read_args"].get("sep", ",")
    date_format = input_settings["spark_read_args"].get("dateFormat", "%d/%m/%Y")

    csv_data = load_csv(input_csv_path, delimiter=delimiter)

    for row in csv_data:
        for column in schema:
            source_col = column["source_column"]
            expected_type = column["type"]
            value = row.get(source_col, "")
            value = clean_value(value)

            if expected_type == "decimal":
                try:
                    row[source_col] = str(float(normalize_decimal(value)))
                except:
                    row[source_col] = ""
            else:
                row[source_col] = value

    with open(output_csv_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_data[0].keys(), delimiter=delimiter)
        writer.writeheader()
        writer.writerows(csv_data)
    print(f"\n💾 Corrected CSV saved to: {output_csv_path}")