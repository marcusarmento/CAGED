import sys
import pandas as pd
sys.path.append("D:/Documentos/CAGED")
from caged_downloader import CagedDownloader

def main():
    caged_downloader = CagedDownloader()
    caged_downloader.install_dependencies()  # Instalar dependências
    caged_downloader.connect()

    # Especificar o ano e os meses desejados
    year = ['2015', '2026', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']  # Adicione os meses desejados aqui

    # Baixar e ler os dados do CAGED
    df_caged = caged_downloader.download_and_read_caged_data(year, months)

    caged_downloader.ftp.quit()  # Desconectar do servidor FTP

    # Aqui você pode usar os dados do DataFrame df_caged conforme necessário
    # Por exemplo, exibir as primeiras linhas do DataFrame
    print(df_caged.head())

    # Agora que você tem os dados no DataFrame df_caged, você pode manipulá-los
    # Aqui está um exemplo de como você pode fazer isso:
    # Salvar os dados em um arquivo CSV
    df_caged.to_csv('dados_caged.csv', index=False)
    print("Dados salvos com sucesso em 'dados_caged.csv'.")

if __name__ == "__main__":
    main()
