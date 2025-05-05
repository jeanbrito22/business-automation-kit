# utils/file_loader.py
import json
import csv
from unidecode import unidecode

def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_csv(path, schema_path, delimiter=","):
    schema = load_schema(schema_path)
    expected_fields = [col["source_column"] for col in schema["table_spec"][0]["schema"]]

    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        def normalize_header(h):
            cleaned = " ".join(h.strip().split()).replace("\u00ba", "º").replace("\u02da", "º")
            return cleaned if cleaned not in expected_fields else next(
                (s for s in expected_fields if unidecode(s) == unidecode(cleaned)), cleaned)

        reader.fieldnames = [normalize_header(h) for h in reader.fieldnames]
        rows = list(reader)

        for row in rows:
            for key in list(row.keys()):
                if key.strip() != key or "  " in key:
                    row[" ".join(key.strip().split())] = row.pop(key)
        return rows
