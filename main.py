import pandas as pd
from caged_downloader import CagedDownloader

def main():
    caged_downloader = CagedDownloader()
    caged_downloader.install_dependencies()  # Instalar dependências
    caged_downloader.connect()

    # Especificar a UF desejada (por exemplo, Alagoas: 27)
    uf = 27

    # Definir a tabela final
    CAGEDMun = pd.DataFrame()

    # Iterar pelos anos e meses
    for year in range(2015, 2024):
        for month in range(1, 13):
            month_str = f'{month:02}'
            data = caged_downloader.SecaoMunicipios(year, month_str, uf)
            CAGEDMun = CAGEDMun.append(data, ignore_index=True)

    caged_downloader.ftp.quit()  # Desconectar do servidor FTP

    # Salvar os dados em um arquivo CSV
    CAGEDMun.to_csv('CAGEDMun.csv', encoding='iso-8859-1', index=False)
    print("Dados salvos com sucesso em 'CAGEDMun.csv'.")

if __name__ == "__main__":
    main()
