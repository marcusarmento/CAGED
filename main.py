import sys
import pandas as pd
from caged_downloader import CagedDownloader

def main():
    caged_downloader = CagedDownloader()
    caged_downloader.install_dependencies()  # Instalar dependências
    caged_downloader.connect()  # Conectar ao servidor FTP

    # Especificar os anos e meses desejados
    years = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    # Filtrar dados para Alagoas (UF: AL) e Maceió (Código IBGE: 2704302)
    uf = 'AL'
    muni = '2704302'

    # Baixar e ler os dados do CAGED
    df_caged = caged_downloader.download_and_read_caged_data(years, months, uf, muni)

    caged_downloader.ftp.quit()  # Desconectar do servidor FTP

    # Exibir as primeiras linhas do DataFrame df_caged
    print(df_caged.head())

    # Salvar os dados em um arquivo CSV
    df_caged.to_csv('dados_caged.csv', index=False)
    print("Dados salvos com sucesso em 'dados_caged.csv'.")

if __name__ == "__main__":
    main()
