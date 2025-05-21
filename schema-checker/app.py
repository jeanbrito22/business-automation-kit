import streamlit as st
from datetime import datetime
from pathlib import Path
import os, re
from interface.uploader import handle_uploads
from interface.mapping_builder import build_excel_mapping_interface
from interface.mapping_builder_grouped import (
    build_grouped_excel_mapping_interface,
    separar_grupos_por_tamanho
)
from interface.schema_matcher import check_csv_schema_compatibility, identify_non_standard_csvs
from interface.runner import run_processing_pipeline

st.set_page_config(page_title="CSV Validator", layout="wide")

st.title("📊 Validador de Dados - CSV & Excel")

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

grupos_agrupados, arquivos_individuais = separar_grupos_por_tamanho(DATA_INPUT_XLSX)

st.subheader("2.1 Mapeamento agrupado por prefixo (ideal para arquivos recorrentes)")
build_grouped_excel_mapping_interface(grupos_agrupados, SCHEMA_DIR, MAPPING_PATH)

st.divider()

st.subheader("2.2 Mapeamento individual (arquivo por arquivo)")
build_excel_mapping_interface(arquivos_individuais, SCHEMA_DIR, MAPPING_PATH)

# Checagem dos CSVs e schemas
# Verifica CSVs sem schema correspondente
st.header("3. Verificação de compatibilidade de schemas")
missing_schemas = check_csv_schema_compatibility(DATA_INPUT_CSV, SCHEMA_DIR)
csvs_fora_do_padrao = identify_non_standard_csvs(DATA_INPUT_CSV, SCHEMA_DIR)

if missing_schemas:
    st.error("Schemas ausentes para os seguintes arquivos CSV:")
    for fname in missing_schemas:
        st.code(fname)
    st.stop()
elif csvs_fora_do_padrao:
    st.warning("Alguns arquivos CSV foram ignorados por não seguirem o padrão esperado:")
    for fname in csvs_fora_do_padrao:
        st.code(fname)
    st.markdown("➡️ Esperado: arquivos com nome no formato `tb_file_<nome>.csv` para que o schema `file_ingestion_<nome>.json` seja aplicado.")
    st.markdown("✅ Você pode renomear o arquivo.")
else:
    st.success("Todos os arquivos CSV possuem schema correspondente.")

# Ações possíveis
st.header("4. Execução completa da pipeline")
st.markdown("Clique no botão abaixo para converter os Excels, validar os CSVs e aplicar correções.")
mode = "executar tudo"

# Define o estado inicial se ainda não estiver definido
if "executado" not in st.session_state:
    st.session_state.executado = False

if st.button("🚀 Executar"):
    st.session_state.executado = True
    run_processing_pipeline(mode, BASE_DIR, DATA_INPUT_CSV, DATA_INPUT_XLSX, SCHEMA_DIR, MAPPING_PATH, DATA_OUTPUT_DIR, DATA_LOG_DIR)
    st.success("Processamento finalizado! Verifique os outputs e logs.")

if st.session_state.executado:
    st.header("5. Visualização dos logs")

    for log_file in sorted(DATA_LOG_DIR.glob("*.log")):
        st.subheader(f"📝 Log: {log_file.name}")
        with open(log_file, "r", encoding="utf-8") as f:
            log_content = f.read()
        st.code(log_content, language="text", height=300)

    st.subheader("📁 Arquivos disponíveis na pasta de outputs")

    arquivos = os.listdir(DATA_OUTPUT_DIR)
    if not arquivos:
        st.info("Nenhum arquivo encontrado na pasta de outputs.")

    for nome_arquivo in sorted(arquivos):
        caminho_arquivo = os.path.join(DATA_OUTPUT_DIR, nome_arquivo)

        if os.path.isfile(caminho_arquivo):
            data_criacao = datetime.fromtimestamp(os.path.getctime(caminho_arquivo))
            data_formatada = data_criacao.strftime("%d/%m/%Y %H:%M:%S")

            st.markdown(f"**{nome_arquivo}** — _criado em {data_formatada}_")
            with open(caminho_arquivo, "rb") as f:
                bytes_data = f.read()
                st.download_button(
                    label=f"Baixar {nome_arquivo}",
                    data=bytes_data,
                    file_name=nome_arquivo,
                    mime="application/octet-stream"
                )
            st.divider()