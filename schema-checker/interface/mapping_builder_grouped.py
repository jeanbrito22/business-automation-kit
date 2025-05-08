
import streamlit as st
import json
from pathlib import Path
from collections import defaultdict
import re

def separar_grupos_por_tamanho(xlsx_dir: Path):
    grupos = defaultdict(list)
    for xlsx_file in xlsx_dir.glob("*.xlsx"):
        prefix = re.split(r"[_-]", xlsx_file.stem)[0].lower()
        grupos[prefix].append(xlsx_file)

    grupos_agrupados = {k: v for k, v in grupos.items() if len(v) > 1}
    arquivos_individuais = [v[0] for k, v in grupos.items() if len(v) == 1]

    return grupos_agrupados, arquivos_individuais

def gerar_nome_output(schema_filename):
    base = schema_filename.replace("file_ingestion_", "").replace(".json", "")
    return f"tb_file_{base}.csv"

def build_grouped_excel_mapping_interface(grupos_agrupados: dict, schema_dir: Path, mapping_path: Path):
    st.markdown("### Mapeamento Agrupado (2+ arquivos com prefixo comum)")

    if not grupos_agrupados:
        st.info("Nenhum grupo com múltiplos arquivos encontrado.")
        return

    schemas_disponiveis = sorted([
        f.name for f in schema_dir.glob("file_ingestion_*.json")
    ])

    mapping = []

    for prefix, arquivos in grupos_agrupados.items():
        st.subheader(f"Grupo: `{prefix}` ({len(arquivos)} arquivos)")

        sheet_name = st.text_input(
            f"Nome da aba comum (ex: Plan1) para `{prefix}`",
            value="Plan1",
            key=f"sheet_{prefix}"
        )

        schema_escolhido = st.selectbox(
            f"Schema para o grupo `{prefix}`",
            schemas_disponiveis,
            key=f"schema_{prefix}"
        )

        expand_cols = []
        usar_expand = st.radio(
            f"Pivotar colunas para `{prefix}`?",
            ["Não", "Sim"],
            key=f"expand_{prefix}",
            horizontal=True
        )

        if usar_expand == "Sim":
            expand_cols_input = st.text_input(
                "Colunas para expand_dates_to (ex: Ano, Mes, Valor)",
                value="Ano, Mes, Valor",
                key=f"expand_input_{prefix}"
            )
            expand_cols = [col.strip() for col in expand_cols_input.split(",") if col.strip()]

        output_csv_name = gerar_nome_output(schema_escolhido)

        for arq in arquivos:
            mapping.append({
                "filename": arq.name,
                "sheet_name": sheet_name,
                "output_csv_name": output_csv_name,
                "expand_dates_to": expand_cols
            })

    if st.button("Salvar mapping agrupado"):
        try:
            with open(mapping_path, "r", encoding="utf-8") as f:
                existing_mapping = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_mapping = []

        combined_mapping = existing_mapping + [m for m in mapping if m not in existing_mapping]

        with open(mapping_path, "w", encoding="utf-8") as f:
            json.dump(combined_mapping, f, indent=2, ensure_ascii=False)
        st.success(f"Mapping agrupado salvo em {mapping_path}")