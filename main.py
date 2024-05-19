import time
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, HourLocator
import requests
from bs4 import BeautifulSoup
import logging
import schedule
import streamlit as st
from threading import Thread

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminho para salvar os dados e o gráfico
DATA_PATH = "tide_data.csv"
PLOT_PATH = "tide_plot.png"

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
    df['Medição'] += 1.36
    df = df.dropna(subset=['Medição'])
    return df

def plot_tide_data(df):
    # Calcular a média móvel (usando uma janela de 5 períodos como exemplo)
    df['Média Móvel'] = df['Medição'].rolling(window=5).mean()

    plt.clf()
    plt.figure(figsize=(18, 9))  # Ajustar o tamanho do gráfico
    plt.plot(df['DD HH:MM'], df['Medição'], linestyle='-', label='Medição')
    plt.plot(df['DD HH:MM'], df['Média Móvel'], linestyle='-', label='Média Móvel', color='orange')

    # Adicionar linha de tendência
    x = np.arange(len(df))
    y = df['Medição'].values
    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)
    plt.plot(df['DD HH:MM'], p(x), linestyle='--', color='red', label='Linha de Tendência')

    plt.gca().xaxis.set_major_locator(HourLocator(interval=1))
    plt.gca().xaxis.set_major_formatter(DateFormatter('%d-%m %H:%M'))
    plt.title('Nível da Lagoa dos Patos (Rio Grande-RS)', fontsize=16)
    plt.xlabel('Data e Hora', fontsize=14)
    plt.ylabel('Nível da lagoa (m)', fontsize=14)
    plt.grid(True)
    plt.xticks(rotation=45, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.tight_layout()
    plt.savefig(PLOT_PATH)  # Salvar o gráfico
    plt.show()

def job():
    logging.info(f"Fetching data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    url = 'https://www.rgpilots.com.br/'
    df = fetch_tide_data_with_requests(url)
    if not df.empty:
        df = process_data(df)
        df.to_csv(DATA_PATH, index=False)
        plot_tide_data(df)
    else:
        logging.warning("No data fetched.")

# Agendar a execução da raspagem de dados a cada 10 minutos
schedule.every(10).minutes.do(job)

def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)

# Iniciar o agendamento em background
t = Thread(target=run_schedule)
t.daemon = True
t.start()

def main():
    # Executar a raspagem imediatamente na inicialização
    job()

    st.title('Nível da Lagoa dos Patos em Rio Grande - RS')
    st.write("""Aplicativo de monitoramento do nível da Lagoa dos Patos no município de Rio Grande.
    Aqui você pode visualizar dados atualizados a cada 10 minutos.
    Os dados são extraídos da plataforma RG Pilots (Praticagem da Barra) a partir da tábua de maré.
    Use o botão abaixo para atualizar manualmente os dados.""")

    if st.button('Atualizar Agora'):
        job()

    # Ler os dados salvos
    df = pd.read_csv(DATA_PATH)
    st.write(df)

    # Mostrar o gráfico salvo
    st.image(PLOT_PATH, use_column_width=True, output_format="PNG")

    # Incluir um rodapé com informações adicionais
    st.write("""
    ---
    **Informações adicionais:**
    - Fonte dos dados: [RGPilots](https://www.rgpilots.com.br/)
    - Este aplicativo foi desenvolvido durante as enchentes no Estado do Rio Grande do Sul com o objetivo
    de ampliar o acesso à informação na região Sul do Estado.
    - Desenvolvido por Nilton Sainz (INCT-ReDem/UFPR)
    """)

    # Botão para o perfil do Instagram
    st.markdown(
        """
        <a href="https://www.instagram.com/niltonsainz/" target="_blank">
            <button>Instagram</button>
        </a>
        """,
        unsafe_allow_html=True
    )

    # Botão para o perfil Lattes
    st.markdown(
        """
        <a href="https://lattes.cnpq.br/7733003139844460" target="_blank">
            <button>Lattes</button>
        </a>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()