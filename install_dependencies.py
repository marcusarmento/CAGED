import subprocess
import sys

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def main():
    # Lista de pacotes necessários
    required_packages = ['ftplib', 'py7zr', 'pandas']

    for package in required_packages:
        try:
            # Tentar importar o pacote
            __import__(package)
            print(f'{package} já está instalado.')
        except ImportError:
            # Se o pacote não estiver instalado, instale-o
            print(f'Instalando {package}...')
            install_package(package)

    print('Todos os pacotes necessários foram instalados com sucesso.')

if __name__ == "__main__":
    main()
