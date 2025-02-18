from coleta import executar_coleta  # Corrigido: Importar a função de coleta
from processamento import executar_processamento
from enriquecimento import executar_enriquecimento
import time

def executar_pipeline():
    print("\n🚀 Iniciando pipeline...\n")

    try:
        # Coletar dados (Bronze)
        print("⏳ Coletando dados (Bronze)...")
        start_time = time.time()
        arquivo_bronze = executar_coleta()  # Função importada de coleta.py
        if not arquivo_bronze:
            raise ValueError("Nenhum arquivo bronze foi gerado.")
        print(f"✅ Arquivo bronze coletado: {arquivo_bronze}")
        print(f"⏰ Tempo de coleta: {time.time() - start_time:.2f} segundos\n")

        # Processar dados (Prata)
        print("⏳ Processando dados (Prata)...")
        start_time = time.time()
        arquivos_prata = executar_processamento([arquivo_bronze])  # Recebe uma lista
        if not arquivos_prata:
            raise ValueError("Nenhum arquivo prata foi gerado.")
        print(f"✅ Arquivos prata processados: {arquivos_prata}")
        print(f"⏰ Tempo de processamento: {time.time() - start_time:.2f} segundos\n")

        # Enriquecer dados (Ouro)
        print("⏳ Enriquecendo dados (Ouro)...")
        start_time = time.time()
        arquivos_ouro = executar_enriquecimento(arquivos_prata)  # Recebe uma lista
        if not arquivos_ouro:
            raise ValueError("Nenhum arquivo ouro foi gerado.")
        print(f"✅ Arquivos ouro enriquecidos: {arquivos_ouro}")
        print(f"⏰ Tempo de enriquecimento: {time.time() - start_time:.2f} segundos\n")

        print("\n✅ Pipeline concluído com sucesso!")

    except Exception as e:
        print(f"\n❌ Ocorreu um erro na execução do pipeline: {e}")

if __name__ == "__main__":
    executar_pipeline()