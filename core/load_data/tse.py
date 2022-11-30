import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import os

from ..utils import solve_path, solve_dir, lst_files



class DownloadDadosTse:

    base_url = 'https://cdn.tse.jus.br/estatistica/sead/'

    endpoint_mapper = dict(
        perfil_sp = 'odsele/perfil_eleitor_secao/perfil_eleitor_secao_ATUAL_SP.zip',
        boletins_urna_2t_sp = 'eleicoes/eleicoes2022/buweb/bweb_2t_SP_311020221535.zip'
        )

    def __init__(self, data_folder:str='data')->None:

        self.folder = solve_dir(data_folder)

    def dataset_to_url(self, dataset_name):

        try:
            return self.endpoint_mapper[dataset_name]
        except KeyError:
            raise ValueError(f'Dataset {dataset_name} not available.'
                            f'Must choose between {self.endpoint_mapper.keys()}')

    def unzip_bytes(self, content:bytes, dirname:str)->None:

        target_dir = solve_path(dirname, self.folder)

        io = BytesIO(content)
        with ZipFile(io, 'r') as zip_ref:
            zip_ref.extractall(target_dir)

    def download_dados_tse(self, endpoint:str, dirname:str)->None:

        url = self.base_url + endpoint

        with requests.get(url) as r:
            content = r.content

        self.unzip_bytes(content, dirname)

    def download_dataset(self, dataset_name:str)->None:

        url = self.dataset_to_url(dataset_name)
        self.download_dados_tse(url, dataset_name)


    def get_csv_file(self, dirname:str)->str:

        dirname = solve_path(dirname, self.folder)
        if not os.path.exists(dirname):
            return None
        files = lst_files(dirname, '.csv')

        if len(files)>0:
            return files[-1]
        
        return None
        

    def pipe_load_file(self, dataset_name, *_, **csv_kwargs)->pd.DataFrame:

        file = self.get_csv_file(dataset_name)

        if file:
            return pd.read_csv(file, **csv_kwargs)
        
        self.download_dataset(dataset_name)
        file = self.get_csv_file(dataset_name)
        return pd.read_csv(file, **csv_kwargs)

    def __call__(self, dataset_name, *_, **csv_kwargs)->pd.DataFrame:

        return self.pipe_load_file(dataset_name, **csv_kwargs)
        
