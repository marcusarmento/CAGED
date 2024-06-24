import os
import pandas as pd
from ftplib import FTP
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

    def download_and_read_caged_data(self, years, months, uf, muni):
        df_all = pd.DataFrame()

        for year in years:
            for month in months:
                remote_directory = f'/pdet/microdados/NOVO CAGED/{year}/{year}{month}'
                remote_archive = f'CAGEDMOV{year}{month}.7z'
                remote_txt = f'CAGEDMOV{year}{month}.txt'

                # Verificar se o arquivo está disponível no servidor FTP
                try:
                    with open(remote_archive, 'wb') as f:
                        self.ftp.retrbinary('RETR ' + os.path.join(remote_directory, remote_archive), f.write)
                except:
                    print(f"O arquivo {remote_archive} não está disponível para o ano {year} e mês {month}.")
                    continue

                # Descompactar arquivo
                with py7zr.SevenZipFile(remote_archive, mode='r') as z:
                    z.extractall()

                # Ler arquivo de texto e filtrar por UF e município
                df_month = self._read_txt_file(remote_txt)
                if 'UF' in df_month.columns and 'Município' in df_month.columns:
                    df_filtered = df_month[(df_month['uf'] == uf) & (df_month['município'] == muni)]
                    df_all = pd.concat([df_all, df_filtered], ignore_index=True)

                # Remover arquivos temporários
                os.remove(remote_archive)
                os.remove(remote_txt)

                print(f"Mês {month} do ano {year} baixado com sucesso.")

        return df_all
