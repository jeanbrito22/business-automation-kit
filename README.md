# 🧪 CSV Validator & Corrector com JSON Schema

Este projeto valida e corrige arquivos `.csv` com base em esquemas JSON (usando o padrão `file_ingestion_*.json`). Também suporta a conversão de arquivos `.xlsx` para `.csv` com seleção de aba.

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
├── main.py                      # Ponto de entrada principal
├── data/inputs/xlsx/excel_mapping.json   # Mapeamento de Excel para CSV
```

---

## 🚀 Como executar

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
```

```bash
python main.py --mode correct
```

```bash
python main.py --mode convert
```

```bash
python main.py --mode convert_correct
```

---

## 📋 Formato do `excel_mapping.json`

```json
[
  {
    "source": "Planilha (2).xlsx",
    "sheet": "dados_bi",
    "target": "tb_file_planilha.csv"
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

---

## 📌 Requisitos

- Python 3.8+
- pandas
- openpyxl (para leitura de `.xlsx`)

Instale com:

```bash
pip install -r requirements.txt
```

---

## 📮 Futuro

- Integração com Streamlit para interface gráfica
- Upload de Excel e escolha de aba via frontend
- Download dos arquivos corrigidos
