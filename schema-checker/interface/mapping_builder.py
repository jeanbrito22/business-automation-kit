
import streamlit as st
import json
import pandas as pd
from pathlib import Path

def build_excel_mapping_interface(arquivos_individuais: list, schema_dir: Path, mapping_path: Path):
    st.markdown("### Mapeamento Individual de arquivos Excel")

    if not arquivos_individuais:
        st.info("Nenhum arquivo individual para mapear.")
        return

    schemas_disponiveis = sorted([
        f.name for f in schema_dir.glob("file_ingestion_*.json")
    ])

    mapping = []

    for xlsx_file in arquivos_individuais:
        st.subheader(f"Arquivo: {xlsx_file.name}")

        xls = pd.ExcelFile(xlsx_file)
        sheet_name = st.selectbox(
            f"Escolha a aba para {xlsx_file.name}",
            xls.sheet_names,
            key=f"sheet_{xlsx_file.name}"
        )

        sample_df = xls.parse(sheet_name, nrows=1)
        st.write("Colunas disponíveis:", list(sample_df.columns))

        expand_cols = []
        usar_expand = st.radio(
            f"Pivotar colunas para `{xlsx_file.name}`?",
            ["Não", "Sim"],
            key=f"expand_{xlsx_file.name}",
            horizontal=True
        )

        if usar_expand == "Sim":
            expand_cols_input = st.text_input(
                "Digite os nomes das colunas para expand_dates_to (ex: Ano, Mes, Valor)", 
                value="Ano, Mes, Valor",
                key=f"expand_{xlsx_file.name}"
            )

            expand_cols = [col.strip() for col in expand_cols_input.split(",") if col.strip()]

        schema_escolhido = st.selectbox(
            f"Schema para {xlsx_file.name}",
            schemas_disponiveis,
            key=f"schema_{xlsx_file.name}"
        )

        output_csv_name = f"tb_file_{Path(schema_escolhido).stem.replace('file_ingestion_', '')}.csv"

        mapping.append({
            "filename": xlsx_file.name,
            "sheet_name": sheet_name,
            "output_csv_name": output_csv_name,
            "expand_dates_to": expand_cols
        })

    if st.button("Salvar mapping individual"):
        try:
            with open(mapping_path, "r", encoding="utf-8") as f:
                existing_mapping = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_mapping = []

        combined_mapping = existing_mapping + [m for m in mapping if m not in existing_mapping]

        with open(mapping_path, "w", encoding="utf-8") as f:
            json.dump(combined_mapping, f, indent=2, ensure_ascii=False)
        st.success(f"Mapping individual salvo em {mapping_path}")