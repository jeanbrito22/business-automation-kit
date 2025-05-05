import streamlit as st
import shutil
from pathlib import Path
import json

def handle_uploads(uploaded_csvs, uploaded_excels, uploaded_schemas, csv_dir: Path, xlsx_dir: Path, schema_dir: Path, uploaded_mapping, mapping_path: Path):
    # Salva CSVs
    for file in uploaded_csvs:
        with open(csv_dir / file.name, "wb") as f:
            f.write(file.getbuffer())

    # Salva Excels
    for file in uploaded_excels:
        with open(xlsx_dir / file.name, "wb") as f:
            f.write(file.getbuffer())

    # Salva Schemas
    for file in uploaded_schemas:
        with open(schema_dir / file.name, "wb") as f:
            f.write(file.getbuffer())

    # Salva Mapping (se enviado)
    if uploaded_mapping:
        with open(mapping_path, "wb") as f:
            f.write(uploaded_mapping.getbuffer())
