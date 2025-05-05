import streamlit as st
import json
import pandas as pd
from pathlib import Path


def build_excel_mapping_interface(xlsx_dir: Path, schema_dir: Path, mapping_path: Path):
    st.markdown("### Mapeamento de arquivos Excel")
    mapping = []

    for xlsx_file in xlsx_dir.glob("*.xlsx"):
        st.subheader(f"Arquivo: {xlsx_file.name}")
        xls = pd.ExcelFile(xlsx_file)
        sheet_name = st.selectbox(f"Escolha a aba para {xlsx_file.name}", xls.sheet_names, key=xlsx_file.name)

        sample_df = xls.parse(sheet_name, nrows=1)
        st.write("Colunas disponíveis:", list(sample_df.columns))

        expand_cols = st.multiselect("Colunas para expand_dates_to (3: Ano, Mes, Valor)", list(sample_df.columns), key=f"expand_{xlsx_file.name}")

        # Nome do CSV que será gerado
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
