import os
import time
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests

def fetch_and_process_data(url):

    print("Acessando função")
    s = Service(r'F:\repos\v4\nivel-lagoa-dos-patos\chromedriver.exe')
    driver = webdriver.Chrome(service=s)
    driver.get(url)

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/section[2]/div/div[1]/div[1]/div/table'))
        )
        headers = [th.get_attribute('textContent').strip() for th in driver.find_elements(By.XPATH, "/html/body/section[2]/div/div[1]/div[1]/div/table/thead/tr/th")]
        rows = driver.find_elements(By.XPATH, "/html/body/section[2]/div/div[1]/div[1]/div/table/tbody/tr")
        data = []
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            row_data = {headers[i]: col.get_attribute('textContent').strip() for i, col in enumerate(cols)}
            data.append(row_data)

        new_df = pd.DataFrame(data)
        try:
            existing_df = pd.read_csv('dados_mare.csv')
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=headers, keep='last')
            combined_df.to_csv('dados_mare.csv', index=False)
        except FileNotFoundError:
            new_df.to_csv('dados_mare.csv', index=False)

    except Exception as e:
        print(f"Erro ao extrair dados: {str(e)}")
    finally:
        driver.quit()

    # Carregar e processar os dados
    try:
        df = pd.read_csv('dados_mare.csv')
        df['DD HH:MM'] = pd.to_datetime(df['DD HH:MM'], format='%d/%m/%Y %H:%M')
        df['Medição'] = df['Medição'].replace('-', np.nan).astype(float).add(1.36)
        df = df.dropna(subset=['Medição'])

        plt.figure(figsize=(14, 7))
        plt.plot(df['DD HH:MM'], df['Medição'], marker='o', linestyle='-')

        # Configurando o formato da data no eixo x e espaçamento dos ticks
        plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m %H:%M'))

        plt.title('Série temporal Lagoa dos Patos (Rio Grande)')
        plt.xlabel('Data e Hora')
        plt.ylabel('Nível da lagoa (m)')
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()

        plt.savefig('grafico.png')  # Salvar o gráfico como imagem
        plt.close()
    except FileNotFoundError:
        print("Arquivo CSV não encontrado.")

url = "https://www.rgpilots.com.br/"
fetch_and_process_data(url)
