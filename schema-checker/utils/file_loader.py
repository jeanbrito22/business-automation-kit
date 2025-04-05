import json
import csv

def load_schema(path):
    with open(path, "r") as f:
        return json.load(f)

def load_csv(path, delimiter=","):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)