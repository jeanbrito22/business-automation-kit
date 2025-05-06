
import streamlit as st
import json
import pandas as pd
from pathlib import Path

def gerar_nome_output(schema_filename):
    base = schema_filename.replace("file_ingestion_", "").replace(".json", "")
    return f"tb_file_{base}.csv"

def build_excel_mapping_interface(xlsx_dir: Path, schema_dir: Path, mapping_path: Path):
    st.markdown("### Mapeamento de arquivos Excel")
    mapping = []

    # Lista schemas disponíveis
    schemas_disponiveis = sorted([
        f.name for f in schema_dir.glob("file_ingestion_*.json")
    ])

    for xlsx_file in xlsx_dir.glob("*.xlsx"):
        st.subheader(f"Arquivo: {xlsx_file.name}")
        xls = pd.ExcelFile(xlsx_file)
        sheet_name = st.selectbox(f"Escolha a aba para {xlsx_file.name}", xls.sheet_names, key=f"sheet_{xlsx_file.name}")

        sample_df = xls.parse(sheet_name, nrows=1)
        st.write("Colunas disponíveis:", list(sample_df.columns))

        use_expand = st.radio(
            "🔄 Deseja transformar colunas de ano/mês/valor em linhas (pivotar)?",
            ["Não", "Sim"],
            key=f"expand_choice_{xlsx_file.name}",
            horizontal=True
        )

        expand_cols = []
        if use_expand == "Sim":
            expand_cols_input = st.text_input(
                "Quais colunas devem ser usadas para transformar em linhas? (ex: Ano, Mes, Valor)",
                value="Ano, Mes, Valor",
                key=f"expand_input_{xlsx_file.name}"
            )
            expand_cols = [col.strip() for col in expand_cols_input.split(",") if col.strip()]

        schema_escolhido = st.selectbox(
            f"Selecione o schema para {xlsx_file.name}",
            schemas_disponiveis,
            key=f"schema_{xlsx_file.name}"
        )

        if schema_escolhido:
            output_csv_name = gerar_nome_output(schema_escolhido)
        else:
            output_csv_name = f"tb_file_{xlsx_file.stem.lower().replace(' ', '_')}.csv"

        mapping.append({
            "filename": xlsx_file.name,
            "sheet_name": sheet_name,
            "output_csv_name": output_csv_name,
            "expand_dates_to": expand_cols
        })

    if st.button("Salvar mapping.json"):
        with open(mapping_path, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        st.success(f"Mapping salvo em {mapping_path}")
