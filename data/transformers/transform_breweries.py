import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from datetime import datetime
import pytz
from dotenv import load_dotenv

load_dotenv()

# Definir configurações do MinIO
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET_BRONZE = 'bronze'
MINIO_BUCKET_SILVER = 'silver'

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform_data(*args, **kwargs):
    # Conectar ao MinIO usando boto3
    s3 = boto3.client(
        's3',
        endpoint_url=f'http://{MINIO_ENDPOINT}',
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    try:
        print("Iniciando o processo de transformação.")

        # Listar arquivos JSON no bucket Bronze
        raw_data_files = s3.list_objects_v2(Bucket=MINIO_BUCKET_BRONZE, Prefix='data/breweries/')
        if 'Contents' not in raw_data_files:
            raise FileNotFoundError("Nenhum arquivo JSON encontrado no bucket de dados brutos (Bronze).")

        # Pegar o arquivo mais recente
        raw_file_key = raw_data_files['Contents'][0]['Key']

        # Baixar o arquivo do MinIO
        response = s3.get_object(Bucket=MINIO_BUCKET_BRONZE, Key=raw_file_key)
        df = pd.read_json(response['Body'])

        # Verificar se a coluna 'state' existe
        if 'state' not in df.columns:
            raise ValueError("A coluna 'state' não existe nos dados brutos.")

        # Verificar se o bucket Silver existe e criar caso não exista
        try:
            s3.head_bucket(Bucket=MINIO_BUCKET_SILVER)
            print(f"Bucket '{MINIO_BUCKET_SILVER}' já existe.")
        except:
            print(f"Bucket '{MINIO_BUCKET_SILVER}' não encontrado. Criando o bucket...")
            s3.create_bucket(Bucket=MINIO_BUCKET_SILVER)

        # Salvar dados como Parquet, particionando por 'state'
        for state, group in df.groupby('state'):
            state_adjusted = state.replace(" ", "_")
            state_file_path = f'data/breweries/{state_adjusted}/breweries_{state_adjusted}.parquet'
            table = pa.Table.from_pandas(group)

            # Usar um buffer em bytes
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer)
            bytes_data = buffer.getvalue().to_pybytes()

            s3.put_object(
                Bucket=MINIO_BUCKET_SILVER,
                Key=state_file_path,
                Body=bytes_data,
                ContentType='application/octet-stream'
            )

            print(f'Dados salvos no MinIO em: {MINIO_BUCKET_SILVER}/{state_file_path}')

    except Exception as e:
        # Armazenar log de erro
        brasilia_tz = pytz.timezone('America/Sao_Paulo')
        error_message = f'{datetime.now(brasilia_tz)}: {str(e)}\n'
        print(f'Ocorreu um erro: {error_message}')

        # Nome do arquivo de log
        log_file_name = f'log_{datetime.now(brasilia_tz).strftime("%Y-%m-%d_%H:%M:%S")}.txt'

        # Salvar o log de erro no MinIO
        s3.put_object(
            Bucket=MINIO_BUCKET_SILVER,
            Key=f'logs/{log_file_name}',
            Body=error_message,
            ContentType='text/plain'
        )

        print(f'Log de erro salvo no MinIO em: {MINIO_BUCKET_SILVER}/logs/{log_file_name}')


if __name__ == "__main__":
    transform_data()
