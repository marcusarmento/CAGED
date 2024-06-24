from setuptools import setup

setup(
    name='caged-downloader',
    version='1.0.0',
    packages=[''],
    url='https://github.com/marcusarmento/CAGED',
    license='MIT',
    author='Marcus Sarmento',
    author_email='sarmentomvs@gmail.com',
    description='Um pacote Python para baixar dados do CAGED.',
    install_requires=[
        'pandas',
        'py7zr',
        'ftplib'
    ],
)
