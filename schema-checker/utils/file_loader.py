# utils/file_loader.py
import json
import csv
from unidecode import unidecode

def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_csv(path, schema_path=None, delimiter=","):
    expected_fields = [col["source_column"] for col in load_schema(schema_path)["table_spec"][0]["schema"]] if schema_path else []

    def normalize_header(h):
        h = h.strip()                    # remove espaços no início/fim
        h = h.strip('"').strip("'")      # remove aspas duplas e simples se estiverem nas pontas
        h = h.replace("°", "º").replace("˚", "º")
        h = " ".join(h.split())          # remove espaços duplicados
        return next(
            (s for s in expected_fields if unidecode(s) == unidecode(h)),
            h
        )


    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        rows = list(reader)

    if not rows:
        return []

    raw_headers = rows[0]
    headers = [normalize_header(h) for h in raw_headers]
    dict_rows = [dict(zip(headers, row)) for row in rows[1:]]

    # Correções de headers com espaços duplicados
    for row in dict_rows:
        for key in list(row.keys()):
            if key.strip() != key or "  " in key:
                row[" ".join(key.strip().split())] = row.pop(key)

    return dict_rows
