
import streamlit as st
from checker.validator import validate_csv_against_schema
from checker.corrector import generate_corrected_csv
from checker.xlsx_converter import convert_excels_to_csv
from pathlib import Path

def run_processing_pipeline(mode, base_dir: Path, csv_dir, xlsx_dir, schema_dir, mapping_path, output_dir, log_dir):
    # Etapa 1: Converter arquivos Excel (se necessário)
    if mode in ["converter", "executar tudo"]:
        with st.status("🚀 Processando arquivos Excel...", expanded=True) as status:
            st.write("Iniciando conversão dos arquivos...")
            convert_excels_to_csv(mapping_path, xlsx_dir, csv_dir)
            st.write("Conversão concluída.")
            status.update(label="✅ Conversão concluída!", state="complete")

    # Etapa 2: Validação e/ou correção de CSVs
    arquivos_csv = list(csv_dir.glob("tb_file_*.csv"))
    total = len(arquivos_csv)
    log_path = log_dir / "validation_report.log"
    append_flag = False

    if total == 0:
        st.warning("Nenhum arquivo CSV encontrado para processar.")
        return

    progresso = st.progress(0, text="🔄 Processando arquivos CSV...")

    for i, csv_file in enumerate(arquivos_csv, start=1):
        table_name = csv_file.stem.replace("tb_file_", "")
        schema_file = schema_dir / f"file_ingestion_{table_name}.json"
        output_file = output_dir / f"{table_name}.csv"

        progresso.progress(i / total, text=f"🔍 {i}/{total} - Processando: {csv_file.name}")

        if not schema_file.exists():
            st.warning(f"⚠️ Schema ausente: {schema_file.name}")
            continue

        if mode in ["validar", "executar tudo"]:
            st.write(f"✅ Validando {csv_file.name}...")
            validate_csv_against_schema(
                schema_path=schema_file,
                csv_path=csv_file,
                log_path=log_path,
                append=append_flag
            )
            append_flag = True

        if mode in ["corrigir", "executar tudo"]:
            st.write(f"🛠 Corrigindo {csv_file.name}...")
            generate_corrected_csv(schema_file, csv_file, output_file)

    progresso.progress(1.0, text="✅ Processamento finalizado!")
