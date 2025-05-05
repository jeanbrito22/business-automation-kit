import streamlit as st
from pathlib import Path
import shutil
import json
from interface.uploader import handle_uploads
from interface.mapping_builder import build_excel_mapping_interface
from interface.schema_matcher import check_csv_schema_compatibility
from interface.runner import run_processing_pipeline

st.set_page_config(page_title="CSV Validator", layout="wide")

st.title("📊 CSV & Excel Data Checker")

# Diretórios base
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SCHEMA_DIR = BASE_DIR / "schema"

DATA_INPUT_DIR = DATA_DIR / "inputs"
DATA_OUTPUT_DIR = DATA_DIR / "outputs"
DATA_LOG_DIR = DATA_DIR / "logs"
DATA_INPUT_CSV = DATA_INPUT_DIR / "csv"
DATA_INPUT_XLSX = DATA_INPUT_DIR / "xlsx"
MAPPING_PATH = DATA_INPUT_XLSX / "excel_mapping.json"

# Garante estrutura mínima
for d in [DATA_INPUT_CSV, DATA_INPUT_XLSX, DATA_OUTPUT_DIR, DATA_LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

st.header("1. Upload dos arquivos")
with st.form("upload_form"):
    uploaded_csvs = st.file_uploader("📁 Arquivos CSV", type=["csv"], accept_multiple_files=True)
    uploaded_excels = st.file_uploader("📁 Arquivos Excel", type=["xlsx"], accept_multiple_files=True)
    uploaded_schemas = st.file_uploader("📁 Schemas JSON", type=["json"], accept_multiple_files=True)
    uploaded_mapping = st.file_uploader("📄 Mapping (opcional, ignora geração interativa se enviado)", type="json")
    submit_uploads = st.form_submit_button("Sobrescrever arquivos")

if submit_uploads:
    handle_uploads(uploaded_csvs, uploaded_excels, uploaded_schemas, DATA_INPUT_CSV, DATA_INPUT_XLSX, SCHEMA_DIR, uploaded_mapping, MAPPING_PATH)
    st.success("Arquivos salvos com sucesso!")

# Interface de mapeamento para Excel (somente se mapping.json não foi fornecido)
st.header("2. Configurar mapping para arquivos Excel")
build_excel_mapping_interface(DATA_INPUT_XLSX, SCHEMA_DIR, MAPPING_PATH)

# Checagem dos CSVs e schemas
st.header("3. Verificação de compatibilidade de schemas")
missing_schemas = check_csv_schema_compatibility(DATA_INPUT_CSV, SCHEMA_DIR)
if missing_schemas:
    st.error("Schemas ausentes para os seguintes arquivos CSV:")
    for fname in missing_schemas:
        st.code(fname)
    st.stop()
else:
    st.success("Todos os arquivos CSV possuem schema correspondente.")

# Ações possíveis
st.header("4. Ações")
mode = st.selectbox("Escolha a ação:", ["validar", "corrigir", "converter", "executar tudo"])
if st.button("🚀 Executar"):
    run_processing_pipeline(mode, BASE_DIR, DATA_INPUT_CSV, DATA_INPUT_XLSX, SCHEMA_DIR, MAPPING_PATH, DATA_OUTPUT_DIR, DATA_LOG_DIR)
    st.success("Processamento finalizado! Verifique os outputs e logs.")

import time
time.sleep(0.5)  # Aguarda o sistema finalizar escrita dos arquivos .log

st.header("5. Visualização dos logs")

for log_file in sorted(DATA_LOG_DIR.glob("*.log")):
    st.subheader(f"📝 Log: {log_file.name}")
    with open(log_file, "r", encoding="utf-8") as f:
        log_content = f.read()
    st.code(log_content, language="text")
