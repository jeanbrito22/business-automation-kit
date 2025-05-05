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
        cleaned = " ".join(h.strip().split()).replace("°", "º").replace("˚", "º")
        return cleaned if cleaned not in expected_fields else next(
            (s for s in expected_fields if unidecode(s) == unidecode(cleaned)), cleaned)

    with open(path, newline='', encoding='utf-8-sig') as f:
        lines = list(csv.reader(f, delimiter=delimiter))
        if not lines:
            return []

        raw_headers = lines[0]
        headers = [normalize_header(h) for h in raw_headers]
        reader = csv.DictReader(lines[1:], fieldnames=headers, delimiter=delimiter)
        rows = list(reader)

    for row in rows:
        for key in list(row.keys()):
            if key.strip() != key or "  " in key:
                row[" ".join(key.strip().split())] = row.pop(key)

    return rows
