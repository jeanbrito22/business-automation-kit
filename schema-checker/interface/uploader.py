
import shutil

def handle_uploads(csvs, excels, schemas, csv_dir, xlsx_dir, schema_dir, uploaded_mapping, mapping_path):
    # Apaga e recria todas as subpastas de data/
    base_data_dir = csv_dir.parent.parent  # Caminho: data/
    for subfolder in ["inputs/csv", "inputs/xlsx", "outputs", "logs"]:
        full_path = base_data_dir / subfolder
        if full_path.exists():
            shutil.rmtree(full_path)
        full_path.mkdir(parents=True, exist_ok=True)

    # Salva arquivos CSV
    if csvs:
        for f in csvs:
            (csv_dir / f.name).write_bytes(f.getbuffer())

    # Salva arquivos Excel
    if excels:
        for f in excels:
            (xlsx_dir / f.name).write_bytes(f.getbuffer())

    # Salva schemas
    if schemas:
        for f in schemas:
            (schema_dir / f.name).write_bytes(f.getbuffer())

    # Salva mapping se enviado
    if uploaded_mapping:
        mapping_path.write_bytes(uploaded_mapping.getbuffer())
