import os
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
from io import StringIO

# URLs das fontes de dados
URLS = {
    "Paranagua": "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo",
    "Santos": "https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/"
}

PASTA_BRONZE = "data/bronze"
os.makedirs(PASTA_BRONZE, exist_ok=True)

def detectar_colunas(df):
    """
    Detecta automaticamente as colunas de Mercadoria e Sentido (Importação/Exportação).
    Retorna o DataFrame com as colunas renomeadas.
    """
    # Mapeia colunas por palavras-chave
    mercadoria_col = [col for col in df.columns if "mercadoria" in col.lower() or "goods" in col.lower()]
    sentido_col = [col for col in df.columns if "opera" in col.lower() or "operat" in col.lower()]

    if not mercadoria_col or not sentido_col:
        raise ValueError("Colunas não encontradas.")

    # Renomeia colunas
    df = df.rename(columns={
        mercadoria_col[0]: "mercadoria",
        sentido_col[0]: "sentido"
    })

    # Filtra apenas as colunas necessárias
    df = df[["mercadoria", "sentido"]]

    # Traduz valores de sentido
    df["sentido"] = df["sentido"].str.upper().replace({
        "EMB": "Exportação",
        "DESC": "Importação",
        "EMBARQUE": "Exportação",
        "DESCARGA": "Importação"
    })

    return df

def coletar_dados_paranagua():
    print("Coletando dados de Paranaguá...")
    tentativas = 3
    for tentativa in range(tentativas):
        try:
            # Configuração do Selenium
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver.get(URLS["Paranagua"])

            # Aguarda a tabela carregar
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))

            # Extrai a tabela
            soup = BeautifulSoup(driver.page_source, "html.parser")
            tabela = soup.find("table")
            if not tabela:
                raise ValueError("Nenhuma tabela encontrada para Paranaguá.")

            # Converte para DataFrame
            df = pd.read_html(StringIO(str(tabela)), header=[0, 1])[0]
            df.columns = [" ".join(col).strip() for col in df.columns]  # Junta cabeçalhos multiníveis

            # Processa colunas automaticamente
            df = detectar_colunas(df)

            # Adiciona metadados
            df["porto"] = "Paranaguá"
            df["data_coleta"] = pd.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")

            driver.quit()
            return df

        except Exception as e:
            print(f"Tentativa {tentativa + 1} falhou: {e}")
            if tentativa == tentativas - 1:
                print("❌ Falha ao coletar dados de Paranaguá após várias tentativas.")
                return None
            time.sleep(5)

def coletar_dados_santos():
    print("Coletando dados de Santos...")
    try:
        # Requisição HTTP
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(URLS["Santos"], headers=headers, verify=False)
        response.raise_for_status()

        # Extrai a tabela
        soup = BeautifulSoup(response.text, "html.parser")
        tabela = soup.find("table")
        if not tabela:
            raise ValueError("Nenhuma tabela encontrada para Santos.")

        # Converte para DataFrame
        df = pd.read_html(StringIO(str(tabela)), header=[0, 1])[0]
        df.columns = ["_".join(col).strip() for col in df.columns]  # Simplifica cabeçalhos

        # Processa colunas automaticamente
        df = detectar_colunas(df)

        # Adiciona metadados
        df["porto"] = "Santos"
        df["data_coleta"] = pd.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")

        return df

    except Exception as e:
        print(f"Erro ao coletar dados de Santos: {e}")
        return None

def executar_coleta():
    dados_finais = []

    # Coleta Paranaguá
    df_paranagua = coletar_dados_paranagua()
    if df_paranagua is not None:
        dados_finais.append(df_paranagua)

    # Coleta Santos
    df_santos = coletar_dados_santos()
    if df_santos is not None:
        dados_finais.append(df_santos)

    if not dados_finais:
        print("❌ Nenhum dado foi coletado.")
        return None

    # Salva os dados
    df_final = pd.concat(dados_finais, ignore_index=True)
    arquivo_bronze = os.path.join(PASTA_BRONZE, "dados_bronze.csv")

    if os.path.exists(arquivo_bronze):
        os.remove(arquivo_bronze)

    df_final.to_csv(arquivo_bronze, index=False, encoding="utf-8-sig")
    print(f"✅ Dados coletados salvos em {arquivo_bronze}")

    return arquivo_bronze