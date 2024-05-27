import time
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.dates import DateFormatter, HourLocator
import requests
from bs4 import BeautifulSoup
import logging
import schedule
import streamlit as st
from threading import Thread
import os

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurar o backend do matplotlib
matplotlib.use('Agg')

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
    df['Medição'] = df['Medição'].astype(float).round(2)  # Arredondar para 2 casas decimais
    
    df = df.dropna(subset=['Medição'])
    df = df.sort_values(by='DD HH:MM')
    return df

def plot_tide_data(df):
    # Calcular a média móvel (usando uma janela de 5 períodos como exemplo)
    df.loc[:, 'Média Móvel'] = df['Medição'].rolling(window=5).mean()

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

    # Definir intervalo de 4 horas para os ticks do eixo x
    plt.gca().xaxis.set_major_locator(HourLocator(interval=4))
    
    # Formatar as datas no eixo x
    date_format = DateFormatter('%d-%m %H:%M')
    plt.gca().xaxis.set_major_formatter(date_format)
    
    plt.title('Nível da Lagoa dos Patos (Rio Grande-RS)', fontsize=16)
    plt.xlabel('Data e Hora', fontsize=14)
    plt.ylabel('Nível da lagoa (m)', fontsize=14)
    plt.grid(True)
    plt.xticks(rotation=45, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.tight_layout()

    # Certifique-se de que o diretório existe
    if os.path.dirname(PLOT_PATH):
        os.makedirs(os.path.dirname(PLOT_PATH), exist_ok=True)
    
    # Salvar o gráfico
    try:
        plt.savefig(PLOT_PATH)
        logging.info(f"Plot saved to {PLOT_PATH}")
    except Exception as e:
        logging.error(f"Error saving plot: {e}")
    finally:
        plt.close()
        
def job():
    logging.info(f"Fetching data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    url = 'https://www.rgpilots.com.br/'
    df_new = fetch_tide_data_with_requests(url)
    if not df_new.empty:
        try:
            # Carregar os dados existentes do arquivo CSV se existirem
            df_existing = pd.read_csv(DATA_PATH, parse_dates=['DD HH:MM'])
            
            # Converter a coluna 'Medição_new' para float e somar 1.36 apenas aos valores que não são igual a '-'
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
# # Agendar a execução da raspagem de dados a cada 10 minutos
# schedule.every(10).minutes.do(job)

# def run_schedule():
#     while True:
#         schedule.run_pending()
#         time.sleep(1)

# # Iniciar o agendamento em background
# t = Thread(target=run_schedule)
# t.daemon = True
# t.start()

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
    
    # Incluir uma explicação sobre a média móvel
    st.write("""
    ### O que é a Média Móvel no gráfico?
    No contexto deste gráfico, a média móvel é usada
    para suavizar as flutuações nos níveis de água da Lagoa dos Patos, proporcionando uma visão mais clara
    das tendências ao longo do tempo.
    """)

    # Incluir um rodapé com informações adicionais
    st.write("""
    ---
    **Informações adicionais:**
    - Este aplicativo foi desenvolvido durante as enchentes no estado do Rio Grande do Sul com o objetivo de ampliar o acesso à informação na região Sul do estado.
    - Fonte dos dados: [RGPilots](https://www.rgpilots.com.br/)
    """)
    # Estilo personalizado para os botões
    button_style = """
    <style>
    .button {
        display: inline-block;
        background-color: #EEEEEE;  /* Cor do fundo do botão */
        color: black !important;  /* Cor do texto */
        padding: 8px 16px;
        text-align: center;
        text-decoration: none;
        border: none;
        border-radius: 8px;
        margin: 5px;
        transition: background-color 0.3s, color 0.3s;
        font-size: 12px;  /* Tamanho da fonte */
    }
    .button:hover {
        background-color: #EEEEEE;  /* Cor do fundo do botão ao passar o mouse */
        color: white !important;  /* Cor do texto ao passar o mouse */
    }
    </style>
    """
    
    # Adiciona o estilo personalizado ao Streamlit
    st.markdown(button_style, unsafe_allow_html=True)

    # Seção de desenvolvedores
    st.write("""
    ---
    ### Desenvolvedores do Projeto:
    **Nilton Sainz**
    - INCT ReDem/UFPR
    """)
    st.markdown(
        """
        <div>
            <a href="https://www.instagram.com/niltonsainz/" target="_blank" class="button">Instagram</a>
            <a href="https://lattes.cnpq.br/7733003139844460" target="_blank" class="button">Lattes</a>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.write("""
    **Luiz Sebastião D'ávila Filho**
    - INCT ReDem/IFRS
    """)
    st.markdown(
        """
        <div>
            <a href="https://www.instagram.com/luizdavilaf/" target="_blank" class="button">Instagram</a>
            <a href="http://lattes.cnpq.br/2161738588666342" target="_blank" class="button">Lattes</a>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
