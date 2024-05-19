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
    df['Medição'] = df['Medição'].astype(float)
    
    df = df.dropna(subset=['Medição'])
    return df

def job():
    logging.info(f"Fetching data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    url = 'https://www.rgpilots.com.br/'
    df_new = fetch_tide_data_with_requests(url)
    if not df_new.empty:
        try:
            # Carregar os dados existentes do arquivo CSV se existirem
            df_existing = pd.read_csv(DATA_PATH)
            
            # Converter as colunas de data para o formato datetime
            df_existing['DD HH:MM'] = pd.to_datetime(df_existing['DD HH:MM'], format='%d/%m/%Y %H:%M')
            df_new['DD HH:MM'] = pd.to_datetime(df_new['DD HH:MM'], format='%d/%m/%Y %H:%M')

            # Converter a coluna 'Medição_new' para float e somar 1.36 apenas aos valores que não são igual a '-'
            df_new['Medição'] = df_new['Medição'].replace('-', np.nan).astype(float) + 1.36
            
            # Mesclar os dados recém-obtidos com os dados existentes
            df_combined = pd.merge(df_existing, df_new, on='DD HH:MM', how='outer', suffixes=('_existing', '_new'))

            # Substituir valores de Medição existentes pelos novos, se a data for igual
            df_combined['Medição'] = df_combined['Medição_new'].combine_first(df_combined['Medição_existing'])
            df_combined['Previsão'] = df_combined['Previsão_new'].combine_first(df_combined['Previsão_existing'])
            df_combined.drop(columns=['Medição_new', 'Previsão_new', 'Medição_existing', 'Previsão_existing'], inplace=True)
        except FileNotFoundError:
            # Se o arquivo CSV não existir, use apenas os dados recém-obtidos
            df_combined = df_new
        
        # Salvar o DataFrame combinado de volta no arquivo CSV mantendo o formato de data original
        df_combined.to_csv(DATA_PATH, index=False, date_format='%d/%m/%Y %H:%M')
        
        # Processar os dados combinados
        df_combined_processed = process_data(df_combined)
    else:
        logging.warning("No data fetched.")

if __name__ == "__main__":
    job()
