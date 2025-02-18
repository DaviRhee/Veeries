import os
import pandas as pd

PASTA_OURO = "data/gold"
os.makedirs(PASTA_OURO, exist_ok=True)

def enriquecer_dados(arquivo_prata):
    print(f"Enriquecendo dados de {arquivo_prata}...")
    try:
        # Verifica se o arquivo existe
        if not os.path.exists(arquivo_prata):
            raise FileNotFoundError(f"Arquivo {arquivo_prata} não encontrado.")

        # Lê o arquivo da camada prata (em CSV)
        df = pd.read_csv(arquivo_prata, encoding="utf-8-sig")  # Usando read_csv

        # Verifica se o DataFrame está vazio
        if df.empty:
            raise ValueError("O DataFrame está vazio.")

        # Adiciona uma coluna de data de processamento
        df["data_processamento"] = pd.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")

        # Salva os dados enriquecidos na camada ouro (em CSV)
        arquivo_ouro = os.path.join(PASTA_OURO, "dados_ouro.csv")  # Alterado para CSV

        # Exclui o arquivo antigo, se existir
        if os.path.exists(arquivo_ouro):
            os.remove(arquivo_ouro)

        df.to_csv(arquivo_ouro, index=False, encoding="utf-8-sig")  # Usando to_csv
        print(f"✅ Dados enriquecidos salvos em {arquivo_ouro}")

        return arquivo_ouro

    except Exception as e:
        print(f"❌ Erro ao enriquecer {arquivo_prata}: {e}")
        return None

def executar_enriquecimento(arquivos_prata):
    arquivos_ouro = []
    for arquivo in arquivos_prata:
        try:
            arquivo_ouro = enriquecer_dados(arquivo)
            if arquivo_ouro:
                arquivos_ouro.append(arquivo_ouro)
        except Exception as e:
            print(f"❌ Erro ao enriquecer {arquivo}: {e}")
    return arquivos_ouro