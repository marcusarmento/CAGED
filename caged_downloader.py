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

    def download_caged_data(self, years, months):
        success_messages = []

        for year in years:
            for month in months:
                remote_directory = f'/pdet/microdados/NOVO CAGED/{year}/{year}{month}'
                remote_archive = f'CAGEDMOV{year}{month}.7z'
                remote_txt = f'CAGEDMOV{year}{month}.txt'

                # Verificar se o arquivo está disponível para download
                if self._file_exists(remote_directory, remote_archive):
                    # Baixar arquivo compactado
                    with open(remote_archive, 'wb') as f:
                        self.ftp.retrbinary('RETR ' + os.path.join(remote_directory, remote_archive), f.write)

                    # Descompactar arquivo
                    with py7zr.SevenZipFile(remote_archive, mode='r') as z:
                        z.extractall()

                    # Remover arquivos temporários
                    os.remove(remote_archive)

                    # Mensagem de sucesso
                    success_messages.append(f"Mês {month}/{year} baixado com sucesso.")
                else:
                    # Mensagem de erro se o arquivo não estiver disponível
                    success_messages.append(f"Mês {month}/{year} ainda não está disponível.")

        return success_messages

    def _file_exists(self, remote_directory, filename):
        file_list = self.ftp.nlst(remote_directory)
        return filename in file_list

    def download_and_read_caged_data(self, years, months, uf=None, muni=None):
        df_all = pd.DataFrame()

        # Baixar os dados
        success_messages = self.download_caged_data(years, months)

        for year in years:
            for month in months:
                remote_txt = f'CAGEDMOV{year}{month}.txt'

                # Ler arquivo de texto se o download foi bem-sucedido
                if f"Mês {month}/{year} baixado com sucesso." in success_messages:
                    df_month = self._read_txt_file(remote_txt)

                    # Filtrar por UF e município, se fornecidos
                    if uf is not None and muni is not None:
                        df_month = df_month[(df_month['uf'] == uf) & (df_month['município'] == muni)]

                    # Empilhar os dados do mês em um DataFrame geral
                    df_all = pd.concat([df_all, df_month], ignore_index=True)

                    # Remover arquivo temporário
                    os.remove(remote_txt)

        return df_all
