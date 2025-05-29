import csv
from unidecode import unidecode
import json


def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_csv(path, schema_path=None, delimiter=","):
    expected_fields = [
        col["source_column"]
        for col in load_schema(schema_path)["table_spec"][0]["schema"]
    ] if schema_path else []

    def normalize_header(h):
        h = h.strip().strip('"').strip("'").replace("°", "º").replace("˚", "º")
        h = " ".join(h.split())
        return next(
            (s for s in expected_fields if unidecode(s) == unidecode(h)),
            h
        )

    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter, quoting=csv.QUOTE_ALL)
        # Normaliza os nomes das colunas
        raw_headers = reader.fieldnames
        if not raw_headers:
            return []

        headers = [normalize_header(h) for h in raw_headers]

        # Reescreve os registros com headers normalizados
        dict_rows = []
        for row in reader:
            dict_rows.append({normalize_header(k): v for k, v in row.items()})

    return dict_rows
