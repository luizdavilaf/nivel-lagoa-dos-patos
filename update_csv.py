import time
from datetime import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminho para salvar os dados
DATA_PATH = "tide_data.csv"

def fetch_tide_data_with_requests(url):
    data = []
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verificar se a requisição foi bem-sucedida
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find('table')
        headers = [th.text.strip() for th in table.find('thead').find_all('th')]
        rows = table.find('tbody').find_all('tr')

        for row in rows:
            cols = row.find_all('td')
            row_data = {headers[i]: col.text.strip() for i, col in enumerate(cols)}
            data.append(row_data)
    except Exception as e:
        logging.error(f"Error fetching tide data: {e}")

    return pd.DataFrame(data)

def process_data(df):
    df['DD HH:MM'] = pd.to_datetime(df['DD HH:MM'], format='%d/%m/%Y %H:%M')
    df['Medição'] = df['Medição'].replace('-', np.nan)
    df['Medição'] = df['Medição'].astype(float).round(2)  # Arredondar para 2 casas decimais
    
    df = df.dropna(subset=['Medição'])
    df = df.sort_values(by='DD HH:MM')
    return df

def job():
    logging.info(f"Fetching data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    url = 'https://www.rgpilots.com.br/'
    df_new = fetch_tide_data_with_requests(url)
    if not df_new.empty:
        try:
            # Carregar os dados existentes do arquivo CSV se existirem
            df_existing = pd.read_csv(DATA_PATH, parse_dates=['DD HH:MM'], dayfirst=True)

            
            # Converter a coluna 'Medição_new' para float e somar 1.36 apenas aos valores que não são igual a '-'
            df_new['DD HH:MM'] = pd.to_datetime(df_new['DD HH:MM'], format='%d/%m/%Y %H:%M')
            df_new['Medição'] = df_new['Medição'].replace('-', np.nan).astype(float) + 1.36
            
            # Mesclar os dados recém-obtidos com os dados existentes
            df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=['DD HH:MM']).sort_values(by='DD HH:MM')

        except FileNotFoundError:
            # Se o arquivo CSV não existir, use apenas os dados recém-obtidos
            df_combined = df_new

        # Processar os dados combinados
        df_combined_processed = process_data(df_combined)
        
        # Salvar o DataFrame combinado de volta no arquivo CSV mantendo o formato de data original
        df_combined_processed.to_csv(DATA_PATH, index=False, date_format='%d/%m/%Y %H:%M')
        
        # Plotar os dados combinados
        plot_tide_data(df_combined_processed)
    else:
        logging.warning("No data fetched.")

if __name__ == "__main__":
    job()
