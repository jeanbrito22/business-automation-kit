
# 🧪 CSV Validator & Corrector com JSON Schema

Este projeto valida e corrige arquivos `.csv` com base em esquemas JSON (usando o padrão `file_ingestion_*.json`). Também suporta a conversão de arquivos `.xlsx` para `.csv` com seleção de aba, via terminal ou interface gráfica feita com Streamlit.

---

## 📁 Estrutura de Pastas

```
📦 seu-projeto/
├── data/
│   ├── inputs/
│   │   ├── xlsx/                # Excel originais (.xlsx)
│   │   └── csv/                 # CSVs convertidos (tb_file_*.csv)
│   ├── outputs/                 # Arquivos corrigidos
│   └── logs/                    # Arquivo de log de validação
├── schema/                      # Schemas JSON (file_ingestion_*.json)
├── checker/                     # Módulos de validação e correção
│   ├── validator.py
│   ├── corrector.py
│   └── xlsx_converter.py
├── interface/
│   ├── app.py                   # Interface Streamlit
│   ├── uploader.py              # Upload de arquivos
│   ├── runner.py                # Execução da pipeline
│   ├── mapping_builder.py       # Criação do excel_mapping.json
│   └── schema_matcher.py        # Validação de schemas e abas
├── main.py                      # Ponto de entrada principal (CLI)
├── requirements.txt
└── data/inputs/xlsx/excel_mapping.json   # Mapeamento de Excel para CSV
```

---

## 🚀 Como executar via terminal (CLI)

### ✅ Modos suportados:

| Modo               | Ação                                                                 |
|--------------------|-------------------------------------------------------------------------|
| `validate`         | Apenas valida os arquivos `.csv`                                        |
| `correct`          | Valida e aplica correções (gera arquivos corrigidos em `outputs/`)       |
| `convert`          | Converte arquivos `.xlsx` para `.csv`, depois valida                    |
| `convert_correct`  | Converte `.xlsx`, depois valida e corrige, salvando em `outputs/`       |

### 💻 Exemplo de uso:

```bash
python main.py --mode validate
python main.py --mode correct
python main.py --mode convert
python main.py --mode convert_correct
```

---

## 🖼️ Interface Gráfica com Streamlit

A interface gráfica permite:

- Upload de múltiplos arquivos `.csv`, `.xlsx` e `excel_mapping.json`
- Criação visual e interativa do `excel_mapping.json`
- Execução do processo completo (convert, validate, correct)
- Visualização dos logs de validação (`.log`)
- Download direto dos arquivos corrigidos (`outputs/`)
- Botão "Sobrescrever arquivos" para limpar e reprocessar os dados

### ▶️ Como executar:

```bash
streamlit run app.py
```

### 🌐 Acessando:

A interface será aberta automaticamente no navegador padrão. Caso não abra, acesse:  
[http://localhost:8501](http://localhost:8501)

### ⚠️ Observações:

- Os arquivos de saída só aparecem após clicar no botão **Executar**
- O botão **Sobrescrever arquivos** limpa os diretórios antes de salvar novos arquivos
- Os arquivos enviados devem seguir o padrão esperado de colunas (conforme schema)

---

## 📋 Formato do `excel_mapping.json`

```json
[
  {
    "filename": "Planilha (2).xlsx",
    "sheet_name": "dados_bi",
    "output_csv_name": "tb_file_planilha.csv",
    "expand_dates_to": [
      "Ano",
      "Mes",
      "Valor"
    ]
  }
]
```

- **`source`**: nome do arquivo `.xlsx` dentro de `data/inputs/xlsx/`
- **`sheet`**: nome exato da aba a ser convertida
- **`target`**: nome de destino do `.csv`, dentro de `data/inputs/csv/`

---

## ✨ Funcionalidades

- Valida dados contra tipos esperados (`string`, `integer`, `decimal`, `date`, `timestamp`)
- Corrige formatos de datas, timestamps, inteiros com ponto, valores monetários e espaços
- Gera relatório de validação unificado em `data/logs/validation_report.log`
- Converte `.xlsx` com seleção de aba para `.csv`
- Interface gráfica moderna com Streamlit

---

## 📌 Requisitos

- Python 3.8+
- pandas
- openpyxl
- streamlit

Instale com:

```bash
pip install -r requirements.txt
```
