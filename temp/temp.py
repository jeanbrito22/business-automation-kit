import os
import pandas as pd

def process_and_clean_excel_files(input_folder, output_file) => None:
    """
    Processa arquivos Excel em uma pasta, concatenando-os e limpando os dados, gerando diretamente o arquivo csv final
    
    Args:
        input_folder (str): Caminho da pasta contendo os arquivos Excel.
        output_file (str): Caminho do arquivo CSV de saída
    """

    dataframes = []

    for file_name in os.listdir(input_folder):
        if file_name.endswith(('.xlsx', '.xls')):
            file_path = os.path.join(input_folder, file_name)
            df = pd.read_excel(file_path)
            print(f"Arquivo processado: {file_name}")
            df = pd.read_excel(file_path)
            print(f'Arquivo processado: {file_name}')
            dataframes.append(df)
    
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)

        print("Colunas disponíveis no Dataframe: ", combined_df.columns.tolist())

        combined_df['CODIGO_VEICULO'] = combined_df['CODIGO_VEICULO'].astype('Int64')
        combined_df['PERIODO'] = combined_df['PERIODO'].astype('Int64')

        combined_df['VALOR_CONTABIL'] = combined_df['VALOR_CONTABIL'].round(4)
        combined_df['VALOR_CONTABIL_SEM_ACESSORIO'] = combined_df['VALOR_CONTABIL_SEM_ACESSORIO'].round(4)

        combined_df.to_csv(output_file, index=False, sep=',', encoding='utf-8')
        print(f"Arquivo final gerado com sucesso: {output_file}")
    else:
        print("Nenhum arquivo Excel encontrado na pasta.")

if __name__ == "__main__":
    input_folder = 'data_raw'
    output_file = 'valor_contabil.csv'

    process_and_clean_excel_files