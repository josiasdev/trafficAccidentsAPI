import sys
import zipfile
import pandas as pd
from pathlib import Path


# Adicione o diretório raiz do projeto ao sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from schemas.BensCandidato import bens_candidato_entity
from config.database import mongodb_client


ZIP_PATH = '/home/rafael/Downloads/bem_candidato_2024.zip'
CSV_FILENAME = 'bem_candidato_2024_BRASIL.csv'


ZIP_PATH_CANDIDATO = '/home/rafael/Downloads/consulta_cand_2024.zip'
CSV_FILENAME_CANDIDATO = 'consulta_cand_2024_BRASIL.csv'

with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
    with zip_ref.open(CSV_FILENAME) as csv_file:
        benscandidato = pd.read_csv(csv_file, sep=';', encoding='cp1252')
        benscandidato = benscandidato[['SQ_CANDIDATO','NR_ORDEM_BEM_CANDIDATO' ,
                                       'DS_TIPO_BEM_CANDIDATO'
                                        ,'DS_BEM_CANDIDATO','VR_BEM_CANDIDATO', 'DT_ULT_ATUAL_BEM_CANDIDATO','HH_ULT_ATUAL_BEM_CANDIDATO' ]]
      
with zipfile.ZipFile(ZIP_PATH_CANDIDATO, 'r') as zip_ref:
    with zip_ref.open(CSV_FILENAME_CANDIDATO) as csv_file:
        candidato = pd.read_csv(csv_file, sep=';', encoding='cp1252')
        candidato = candidato[['NR_TITULO_ELEITORAL_CANDIDATO', 'SQ_CANDIDATO']]
  

# Mesclar os DataFrames com base na chave comum 'SQ_CANDIDATO'
merged_df = pd.merge(benscandidato, candidato, on='SQ_CANDIDATO')

print(merged_df.head())
print('\n\n')
print(merged_df.dtypes)
print('\n\n')
print(merged_df.count())
print('\n\n')
print(merged_df.isnull().sum())

db = mongodb_client['Candidatos']
collection = db['bens_candidato']

for _, row in merged_df.iterrows():
    entity = bens_candidato_entity(row.to_dict())
    collection.insert_one(entity)

print("Dados inseridos com sucesso!")