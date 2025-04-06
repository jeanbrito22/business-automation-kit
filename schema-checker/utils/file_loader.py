import json
import csv

def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_csv(path, delimiter=","):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)
        reader.fieldnames = [" ".join(h.strip().split()) for h in reader.fieldnames]
        for row in rows:
            for key in list(row.keys()):
                if key.strip() != key or "  " in key:
                    row[" ".join(key.strip().split())] = row.pop(key)
        return rows