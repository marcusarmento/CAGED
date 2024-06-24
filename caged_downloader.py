# caged_downloader.py

from ftplib import FTP
import os
import shutil
import pandas as pd
import py7zr
import subprocess

class CagedDownloader:
    def __init__(self):
        self.host = "ftp.mtps.gov.br"
        self.ftp = FTP()

    def install_dependencies(self):
        # Script para instalar dependências necessárias
        subprocess.run(["python", "install_dependencies.py"])

    def connect(self):
        self.ftp.connect(self.host)
        self.ftp.login(user="anonymous", passwd="")

    def _extract_txt_file(self, archive_path, txt_path):
        with py7zr.SevenZipFile(archive_path, mode='r') as z:
            z.extract(targets=txt_path)

    def _read_txt_file(self, txt_path):
        df = pd.read_csv(txt_path, delimiter=';')
        return df

    def filter_data(self, df, uf=None, muni=None):
        # Aplicar filtro por UF e código IBGE do município, se fornecidos
        if uf:
            df = df[df['uf'] == uf]
        if ibge_code:
            df = df[df['município'] == muni]
        return df

    def download_caged_data(self, years, months, uf=None, muni=None):
        success_messages = []
        for year in years:
            for month in months:
                remote_directory = f'/pdet/microdados/NOVO CAGED/{year}/{year}{month}'
                remote_archive = f'CAGEDMOV{year}{month}.7z'
                remote_txt = f'CAGEDMOV{year}{month}.txt'

                # Tentar baixar o arquivo compactado
                try:
                    with open(remote_archive, 'wb') as f:
                        self.ftp.retrbinary('RETR ' + os.path.join(remote_directory, remote_archive), f.write)
                except:
                    print(f"Mês {month} do ano {year} não está disponível.")
                    continue

                # Descompactar arquivo
                with py7zr.SevenZipFile(remote_archive, mode='r') as z:
                    z.extractall()

                # Ler arquivo de texto e empilhar em um DataFrame
                df_month = self._read_txt_file(remote_txt)

                # Filtrar dados por UF e código IBGE, se fornecidos
                filtered_df = self.filter_data(df_month, uf, ibge_code)

                # Se houver dados filtrados, processar ou salvar como necessário
                if not filtered_df.empty:
                    # Processar os dados filtrados aqui conforme necessário
                    # Por exemplo, você pode salvar ou processar os dados filtrados
                    # Para este exemplo, apenas exibiremos a contagem de registros
                    print(f"Dados do mês {month} do ano {year} foram filtrados com sucesso.")
                    print(f"Total de registros: {len(filtered_df)}")

                    # Salvar os dados filtrados em um arquivo CSV
                    filtered_df.to_csv(f'dados_filtrados_{year}{month}.csv', index=False)
                    success_messages.append(f"Mês {month} do ano {year} foi baixado e filtrado com sucesso.")
                else:
                    print(f"Nenhum dado encontrado para o mês {month} do ano {year}.")

                # Remover arquivos temporários
                os.remove(remote_archive)
                os.remove(remote_txt)

        return success_messages