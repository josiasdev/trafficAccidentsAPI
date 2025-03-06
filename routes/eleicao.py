from fastapi import APIRouter, HTTPException, Query, Request, status, Depends, File, UploadFile # File e Upload para envio de arquivos 
from typing import Annotated
from models.eleicao import EleicaoCreate, EleicaoBase, EleicaoPublic, EleicaoUpdate
from pymongo.collection import Collection
from pymongo import ReturnDocument
from utils.utils import validate_object_id
from schemas.eleicao import eleicao_entity_from_db, eleicao_entities_from_db, eleicao_entity

# Para abrir os arquivos zip e csv
import zipfile 
from io import BytesIO
import pandas as pd

ERROR_DETAIL = "Some error occurred: {e}"
NOT_FOUND = "Not found"

async def get_eleicao_collection(request: Request) -> Collection:
    """Returns the eleicao collection from MongoDB"""
    return request.app.database["eleicao"]

EleicaoCollection = Annotated[Collection, Depends(get_eleicao_collection)]

router = APIRouter()

# Função para tratar o arquivo ZIP de candidatos
async def tratar_zip_candidatos(candidatos_file):
    # Definição das colunas desejadas para os candidatos
    colunas_desejadas_candidatos = [
        "CD_ELEICAO", "DS_ELEICAO", "DT_ELEICAO", "ANO_ELEICAO", 
        "CD_TIPO_ELEICAO", "NM_TIPO_ELEICAO", "TP_ABRANGENCIA", "NR_TURNO"
    ]

    # Carregar o arquivo ZIP de Candidatos
    dados_zip_candidatos = await candidatos_file.read()
    with zipfile.ZipFile(BytesIO(dados_zip_candidatos), 'r') as zip_ref_candidatos:
        # Filtrar apenas os arquivos CSV que contêm "BRASIL" no nome
        arquivos_candidatos = [f for f in zip_ref_candidatos.namelist() if f.endswith('.csv') and 'BRASIL' in f]
        
        # Se não encontrar nenhum arquivo com "BRASIL" no nome, lançar erro
        if not arquivos_candidatos:
            raise HTTPException(status_code=400, detail="Nenhum arquivo CSV contendo 'BRASIL' no nome foi encontrado no ZIP.")
        
        dataframes_candidatos = []
        for arquivo_candidato in arquivos_candidatos:
            with zip_ref_candidatos.open(arquivo_candidato) as csv_file_candidato:
                try:
                    # Ler o CSV com as colunas desejadas
                    df_eleicao = pd.read_csv(csv_file_candidato, sep=';', encoding='latin1', usecols=colunas_desejadas_candidatos)
                    dataframes_candidatos.append(df_eleicao)
                except ValueError as e:
                    raise HTTPException(status_code=400, detail=f"Erro ao ler o arquivo CSV de candidatos: {str(e)}")
        
        # Concatenar todos os DataFrames de candidatos
        df_candidatos = pd.concat(dataframes_candidatos, ignore_index=True)
  
    # Remover duplicatas com base no código da eleição    
    df_eleicao_unico = df_eleicao.drop_duplicates(subset=['CD_ELEICAO'])
    df_eleicao_unico.columns = df_eleicao_unico.columns.str.lower()
   
    return df_eleicao_unico

# ------------ ROTAS ------------
# Endpoint para upload dos dados da eleição
@router.post("/upload/carregar-dados-eleicao", 
    response_description="Import data to DB using a ZIP file.", 
    status_code=status.HTTP_201_CREATED)
async def upload_dados_eleicao(
        eleicao_collection: EleicaoCollection,
    candidatos_file: UploadFile = File(...)
):
    if not candidatos_file:
        raise HTTPException(status_code=400, detail="É necessário enviar um arquivo ZIP.")

    try:
        # Processar os dados do arquivo ZIP
        df_eleicao_unico = await tratar_zip_candidatos(candidatos_file)

        # Inserir os registros no banco de dados
        for eleicao in df_eleicao_unico.to_dict(orient="records"):
            dados_eleicao = eleicao_entity(eleicao)

            # Verificar se já existe no banco de dados
            if eleicao_collection.find_one({'cd_eleicao': dados_eleicao['cd_eleicao']}):
                raise HTTPException(
                    status_code=400, 
                    detail=f"Eleição com cd_eleicao {dados_eleicao['cd_eleicao']} já existe no banco de dados."
                )

            # Inserir no banco de dados
            eleicao_collection.insert_one(dados_eleicao)

        return {"message": "Dados importados com sucesso!", "total_registros": len(df_eleicao_unico)}

    except HTTPException as http_exc:
        raise http_exc  # Mantém o status correto

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", 
    response_description="Registers a new Eleicao", 
    status_code=status.HTTP_201_CREATED, response_model=EleicaoCreate)
async def create_eleicao(eleicao_collection: EleicaoCollection, eleicao: EleicaoCreate):
    try:
        eleicao_data = eleicao.model_dump()

        # Verifica se já tem o cd_eleicao cadastrado.
        if eleicao_collection.find_one({'cd_eleicao': eleicao_data['cd_eleicao']}) is not None:
            raise HTTPException(status_code=400, detail=f"Eleição com cd_eleicao {eleicao_data['cd_eleicao']} já existe no banco de dados.")
          
        eleicao_collection.insert_one(eleicao_data)
        
        return eleicao_entity_from_db(created)
    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        raise HTTPException(status_code=500, detail=ERROR_DETAIL.format(e=e))