import os
import pandas as pd

PASTA_PRATA = "data/silver"
os.makedirs(PASTA_PRATA, exist_ok=True)

def processar_dados(arquivo_bronze):
    print(f"Processando dados de {arquivo_bronze}...")
    try:
        # Verifica se o arquivo existe
        if not os.path.exists(arquivo_bronze):
            raise FileNotFoundError(f"Arquivo {arquivo_bronze} não encontrado.")

        # Lê o arquivo bronze
        df = pd.read_csv(arquivo_bronze, encoding="utf-8-sig")

        # Verifica colunas necessárias
        colunas_necessarias = ["mercadoria", "sentido", "porto", "data_coleta"]
        if not all(col in df.columns for col in colunas_necessarias):
            missing = [col for col in colunas_necessarias if col not in df.columns]
            raise ValueError(f"Colunas faltantes: {missing}")

        # Normaliza os dados
        df["mercadoria"] = df["mercadoria"].str.strip().str.upper()
        df["sentido"] = df["sentido"].str.strip().str.upper()
        df["porto"] = df["porto"].str.strip().str.upper()

        # Remove duplicatas
        df.drop_duplicates(inplace=True)

        # Salva o arquivo prata
        arquivo_prata = os.path.join(PASTA_PRATA, "dados_prata.csv")
        if os.path.exists(arquivo_prata):
            os.remove(arquivo_prata)
        df.to_csv(arquivo_prata, index=False, encoding="utf-8-sig")

        print(f"✅ Dados processados salvos em {arquivo_prata}")
        return arquivo_prata

    except Exception as e:
        print(f"❌ Erro ao processar {arquivo_bronze}: {e}")
        return None

def executar_processamento(arquivos_bronze):
    arquivos_prata = []
    for arquivo in arquivos_bronze:
        try:
            arquivo_prata = processar_dados(arquivo)
            if arquivo_prata:
                arquivos_prata.append(arquivo_prata)
        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")
    return arquivos_prata