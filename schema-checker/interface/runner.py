
import streamlit as st
from checker.validator import validate_csv_against_schema
from checker.corrector import generate_corrected_csv
from checker.xlsx_converter import convert_single_excel_to_csv
from pathlib import Path

def run_processing_pipeline(mode, base_dir: Path, csv_dir, xlsx_dir, schema_dir, mapping_path, output_dir, log_dir):
    if mode in ["converter", "executar tudo"]:
        arquivos_excel = list(xlsx_dir.glob("*.xlsx"))
        total_excel = len(arquivos_excel)

        if total_excel == 0:
            st.warning("Nenhum arquivo Excel encontrado para conversão.")
        else:
            progresso_excel = st.progress(0, text="⏳ Iniciando conversão dos arquivos Excel...")

            for i, excel_file in enumerate(arquivos_excel, start=1):
                st.write(f"📄 ({i}/{total_excel}) Convertendo: `{excel_file.name}`")
                convert_single_excel_to_csv(excel_file, mapping_path, xlsx_dir, csv_dir)
                progresso_excel.progress(i / total_excel, text=f"✅ {i}/{total_excel} convertidos")

            progresso_excel.progress(1.0, text="🎉 Conversão de Excel finalizada!")

    log_path = log_dir / "validation_report.log"
    append_flag = False

    arquivos_csv = list(csv_dir.glob("tb_file_*.csv"))
    total_csv = len(arquivos_csv)

    if total_csv > 0:
        progresso = st.progress(0, text="⏳ Processando arquivos CSV...")

        for i, csv_file in enumerate(arquivos_csv, start=1):
            table_name = csv_file.stem.replace("tb_file_", "")
            schema_file = schema_dir / f"file_ingestion_{table_name}.json"
            output_file = output_dir / f"{table_name}.csv"

            if not schema_file.exists():
                continue

            if mode in ["validar", "executar tudo"]:
                validate_csv_against_schema(
                    schema_path=schema_file,
                    csv_path=csv_file,
                    log_path=log_path,
                    append=append_flag
                )
                append_flag = True

            if mode in ["corrigir", "executar tudo"]:
                generate_corrected_csv(schema_file, csv_file, output_file)

            progresso.progress(i / total_csv, text=f"✅ {i}/{total_csv} arquivos CSV processados")

        progresso.progress(1.0, text="🎉 Processamento de CSV finalizado!")
    else:
        st.warning("Nenhum arquivo CSV encontrado para validação ou correção.")

