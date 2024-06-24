import os
import shutil
import pandas as pd
from ftplib import FTP
import py7zr
import subprocess

class CagedDownloader:
    def __init__(self):
        self.host = "ftp.mtps.gov.br"
        self.ftp = FTP()

    def install_dependencies(self):
        # Executar o script install_dependencies.py para garantir que os pacotes necessários sejam instalados
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

    def download_and_read_caged_data(self, year, months):
        df_all = pd.DataFrame()  # DataFrame que armazenará todos os dados empilhados

        for month in months:
            remote_directory = f'/pdet/microdados/NOVO CAGED/{year}/{year}{month}'
            remote_archive = f'CAGEDMOV{year}{month}.7z'
            remote_txt = f'CAGEDMOV{year}{month}.txt'

            # Baixar arquivo compactado
            with open(remote_archive, 'wb') as f:
                self.ftp.retrbinary('RETR ' + os.path.join(remote_directory, remote_archive), f.write)

            # Descompactar arquivo
            with py7zr.SevenZipFile(remote_archive, mode='r') as z:
                z.extractall()

            # Ler arquivo de texto e empilhar em um DataFrame
            df_month = self._read_txt_file(remote_txt)

            # Empilhar os dados do mês em um DataFrame geral
            df_all = pd.concat([df_all, df_month], ignore_index=True)

            # Remover arquivos temporários
            os.remove(remote_archive)
            os.remove(remote_txt)

        return df_all