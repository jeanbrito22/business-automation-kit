import json
import csv
from unidecode import unidecode

def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_csv(path, delimiter=","):
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter=delimiter)
        lines = list(reader)

    # Header normalizado
    raw_headers = lines[0]
    schema = load_schema("schema/sample_schema.json")
    expected_fields = [col["source_column"] for col in schema["table_spec"][0]["schema"]]

    def normalize_header(h):
        cleaned = " ".join(h.strip().split()).replace("°", "º").replace("˚", "º")
        return cleaned if cleaned not in expected_fields else next((s for s in expected_fields if unidecode(s) == unidecode(cleaned)), cleaned)

    normalized_headers = [normalize_header(h) for h in raw_headers]

    corrected_lines = []

    for row in lines[1:]:
        if len(row) == len(normalized_headers) - 1:
            # Linha com separador a menos (primeira coluna faltando)
            corrected_lines.append([""] + row)
        else:
            corrected_lines.append(row)

    # Usa DictReader com os headers normalizados
    rows = []
    for values in corrected_lines:
        row = dict(zip(normalized_headers, values))
        # Limpa espaços duplicados nas chaves
        clean_row = {}
        for key, val in row.items():
            key_clean = " ".join(key.strip().split())
            clean_row[key_clean] = val
        rows.append(clean_row)

    return rows
