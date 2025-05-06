import streamlit as st
from checker.validator import validate_csv_against_schema
from checker.corrector import generate_corrected_csv
from checker.xlsx_converter import convert_excels_to_csv
from pathlib import Path

def run_processing_pipeline(mode, base_dir: Path, csv_dir, xlsx_dir, schema_dir, mapping_path, output_dir, log_dir):
    if mode in ["converter", "executar tudo"]:
        with st.status("🚀 Processando arquivos Excel...", expanded=True) as status:
            st.write("Iniciando conversão dos arquivos...")
            convert_excels_to_csv(mapping_path, xlsx_dir, csv_dir)
            st.write("Finalizando...")
            status.update(label="✅ Processamento concluído!", state="complete")

    for csv_file in csv_dir.glob("tb_file_*.csv"):
        table_name = csv_file.stem.replace("tb_file_", "")
        schema_file = schema_dir / f"file_ingestion_{table_name}.json"
        output_file = output_dir / f"{table_name}.csv"

        if not schema_file.exists():
            continue

        if mode in ["validar", "executar tudo"]:
            validate_csv_against_schema(schema_path=schema_file, csv_path=csv_file)

        if mode in ["corrigir", "executar tudo"]:
            generate_corrected_csv(schema_file, csv_file, output_file)
