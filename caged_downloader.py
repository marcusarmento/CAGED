import os
from ftplib import FTP
import pandas as pd
import py7zr

class CagedDownloader:
    def __init__(self):
        self.host = "ftp.mtps.gov.br"
        self.ftp = FTP()

    def install_dependencies(self):
        import subprocess
        subprocess.run(["pip", "install", "-r", "requirements.txt"])

    def connect(self):
        self.ftp.connect(self.host)
        self.ftp.login(user="anonymous", passwd="")

    def _extract_txt_file(self, archive_path, txt_path):
        with py7zr.SevenZipFile(archive_path, mode='r') as z:
            z.extract(targets=txt_path)

    def _read_txt_file(self, txt_path):
        df = pd.read_csv(txt_path, delimiter=';', encoding='latin1')
        return df

    def _download_and_filter_data(self, year, month, uf, level):
        df_all = pd.DataFrame()
        remote_directory = f'/pdet/microdados/NOVO CAGED/{year}/{year}{month}'
        remote_archive = f'CAGEDMOV{year}{month}.7z'
        remote_txt = f'CAGEDMOV{year}{month}.txt'

        try:
            with open(remote_archive, 'wb') as f:
                self.ftp.retrbinary('RETR ' + os.path.join(remote_directory, remote_archive), f.write)
        except:
            print(f"O arquivo {remote_archive} não está disponível para o ano {year} e mês {month}.")
            return df_all

        with py7zr.SevenZipFile(remote_archive, mode='r') as z:
            z.extractall()

        df_month = self._read_txt_file(remote_txt)
        if 'UF' in df_month.columns:
            df_filtered = df_month[df_month['UF'] == uf]
            df_all = pd.concat([df_all, df_filtered], ignore_index=True)

        os.remove(remote_archive)
        os.remove(remote_txt)
        print(f"Mês {month} do ano {year} baixado com sucesso.")

        if level == 'Subclasse':
            return df_all[df_all['CNAE 2.0 Subclasse'] != '']
        elif level == 'Classe':
            return df_all[df_all['CNAE 2.0 Classe'] != '']
        elif level == 'Seção':
            return df_all[df_all['CNAE 2.0 Seção'] != '']

    def SubclasseMunicipios(self, year, month, uf):
        return self._download_and_filter_data(year, month, uf, 'Subclasse')

    def ClasseMunicipios(self, year, month, uf):
        return self._download_and_filter_data(year, month, uf, 'Classe')

    def SecaoMunicipios(self, year, month, uf):
        return self._download_and_filter_data(year, month, uf, 'Seção')
